from flask import Flask, request, jsonify
from neo4j import GraphDatabase

def create_app():
    app = Flask(__name__)
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "Hdaqp2hm$"))

    @app.route('/cities', methods=['PUT'])
    def add_citys():
        data = request.json
        name = data["name"]
        country = data["country"]

        if not name:
            return "Could not register city, it exists or mandatory attributes are missing", 400

        if not country:
            return "Could not register city, it exists or mandatory attributes are missing", 400

        with driver.session() as session:
        # Check if the city already exists
            result = session.run(
            """
            MATCH (c:City {name: $name, country: $country})
            RETURN c
            """, name=name, country=country
            )

            if result.single():
                return "Could not register city, it exists or mandatory attributes are missing", 400 

        # Create the city if it doesn't exist
            session.run(
            """
            CREATE (c:City {name: $name, country: $country})
            """, name=name, country=country
            )

            return "City registered successfully", 201
        
    @app.route('/cities', methods=['GET'])
    def get_all_cities():
        country = request.args.get("country")  # Retrieve the 'country' query parameter if provided

        with driver.session() as session:
            if country:
            # Query to get cities filtered by the specified country
                result = session.run(
                    """
                    MATCH (c:City {country: $country})
                    RETURN c.name AS name, c.country AS country
                    """, country=country
                )
            else:
                # Query to get all cities without filtering
                result = session.run(
                    """
                    MATCH (c:City)
                    RETURN c.name AS name, c.country AS country
                    """
                )

            cities = [{"name": record["name"], "country": record["country"]} for record in result]

        return jsonify(cities), 200
    
    @app.route('/cities/<name>', methods=['GET'])
    def get_city(name):
        with driver.session() as session:
        # Query to match a city by its name
            result = session.run(
                """
                MATCH (c:City {name: $name})
                RETURN c.name AS name, c.country AS country
                """, name=name
            )

        # Extract the single record if it exists
            record = result.single()

            if record:
                city = {
                    "name": record["name"],
                    "country": record["country"]
                }
                return (city), 200
            else:
                return "City not found", 404
            
    
    @app.route('/cities/<name>/airports', methods=['PUT'])
    def register_airport(name):
        data = request.json
        required_fields = ["code", "name", "numberOfTerminals", "address"]

    # Check if all required fields are present
        if not all(field in data for field in required_fields):
            return "Airport could not be created due to missing data or city ther airport is registered in is not registered in the system", 400

        with driver.session() as session:
        # Check if the city exists
            city_check = session.run(
                """
                MATCH (c:City {name: $name})
                RETURN c
                """, name=name
            ).single()

            if not city_check:
                return "Airport could not be created due to missing data or city ther airport is registered in is not registered in the system", 400

            
            airport_global_check = session.run(
                """
                MATCH (a:Airport {code: $code})
                RETURN a
                """,
                code=data["code"]
            ).single()

            if airport_global_check:
                return "Airport could not be created due to missing data or city ther airport is registered in is not registered in the system", 400

        # Create the airport node and relationship with the city
            session.run(
                """
                MATCH (c:City {name: $name})
                CREATE (a:Airport {
                    code: $code,
                    name: $airport_name,
                    numberOfTerminals: $numberOfTerminals,
                    address: $address
                })
                CREATE (c)-[:HAS_AIRPORT]->(a)
                """,
                name=name,
                code=data["code"],
                airport_name=data["name"],
                numberOfTerminals=data["numberOfTerminals"],
                address=data["address"]
            )

            return "Airport created", 201
        
    @app.route('/cities/<name>/airports', methods=['GET'])
    def get_airports_in_city(name):
        with driver.session() as session:
            
        # Retrieve all airports related to the city
            result = session.run(
                """
                MATCH (c:City {name: $name})-[:HAS_AIRPORT]->(a:Airport)
                RETURN a.code AS code, c.name AS city, a.name AS airport_name,
                   a.numberOfTerminals AS numberOfTerminals, a.address AS address
                """, name=name
            )

            airports = [{
                "code": record["code"],
                "city": record["city"],
                "name": record["airport_name"],
                "numberOfTerminals": record["numberOfTerminals"],
                "address": record["address"]
            } for record in result]

            return jsonify(airports), 200

    
    @app.route('/airports/<code>', methods=['GET'])
    def get_airport_by_code(code):
        with driver.session() as session:
        # Query to match an airport by its code and include the related city
            result = session.run(
                """
                MATCH (c:City)-[:HAS_AIRPORT]->(a:Airport {code: $code})
                RETURN a.code AS code, c.name AS city, a.name AS airport_name,
                       a.numberOfTerminals AS numberOfTerminals, a.address AS address
                """, code=code
            )

        # Extract the single record if it exists
            record = result.single()

            if record:
                airport = {
                    "code": record["code"],
                    "city": record["city"],
                    "name": record["airport_name"],
                    "numberOfTerminals": record["numberOfTerminals"],
                    "address": record["address"]
                }
                return (airport), 200
            else:
                return "City not found", 404

    @app.route('/flights', methods=['PUT'])
    def register_flight():
        data = request.json
        required_fields = ["number", "fromAirport", "toAirport", "price", "flightTimeInMinutes", "operator"]

    # Validate that all required fields are present
        if not all(field in data for field in required_fields):
            return "Flight not created due to missing data", 400

        flight_number = data["number"]
        from_airport = data["fromAirport"]
        to_airport = data["toAirport"]
        price = data["price"]
        flight_time = data["flightTimeInMinutes"]
        operator = data["operator"]

        with driver.session() as session:
        # Check if both airports exist
            from_airport_check = session.run(
                """
                MATCH (c:City)-[:HAS_AIRPORT]->(a:Airport {code: $from_airport})
                RETURN a
                """, from_airport=from_airport
            ).single()

            to_airport_check = session.run(
                """
                MATCH (c:City)-[:HAS_AIRPORT]->(a:Airport {code: $to_airport})
                RETURN a
                """, to_airport=to_airport
            ).single()

            if not from_airport_check:
                return f"Departure airport '{from_airport}' not found", 404

            if not to_airport_check:
                return f"Arrival airport '{to_airport}' not found", 404

        # Check if the flight already exists
            flight_check = session.run(
                """
                MATCH (:Airport {code: $from_airport})-[:FLIGHT_TO]->(f:Flight)-[:ARRIVES_AT]->(:Airport {code: $to_airport})
                WHERE f.number = $flight_number
                RETURN f
                """, from_airport=from_airport, to_airport=to_airport, flight_number=flight_number
            ).single()

            if flight_check:
                return "Flight with the same number already exists between these airports", 400

        # Create the flight node and relationship
            session.run(
                """
                MATCH (from:Airport {code: $from_airport}), (to:Airport {code: $to_airport})
                CREATE (from)-[:FLIGHT_TO]->(f:Flight {
                    number: $flight_number,
                    price: $price,
                    flightTimeInMinutes: $flight_time,
                    operator: $operator
                })-[:ARRIVES_AT]->(to)
                """, 
                from_airport=from_airport,
                to_airport=to_airport,
                flight_number=flight_number,
                price=price,
                flight_time=flight_time,
                operator=operator
            )

            return "Flight created", 201

    @app.route('/flights/<code>', methods=['GET'])
    def get_flight_by_code(code):
        with driver.session() as session:
        # Query to match the flight and its associated airports and cities
            result = session.run(
                """
                MATCH (fromCity:City)-[:HAS_AIRPORT]->(fromAirport:Airport)-[:FLIGHT_TO]->(f:Flight {number: $code})-[:ARRIVES_AT]->(toAirport:Airport)<-[:HAS_AIRPORT]-(toCity:City)
                RETURN 
                    f.number AS number,
                    fromAirport.code AS fromAirport,
                    fromCity.name AS fromCity,
                    toAirport.code AS toAirport,
                    toCity.name AS toCity,
                    f.price AS price,
                    f.flightTimeInMinutes AS flightTimeInMinutes,
                    f.operator AS operator
                """,
                code=code
            )

        # Extract the single record if it exists
            record = result.single()

            if record:
                flight_info = {
                    "number": record["number"],
                    "fromAirport": record["fromAirport"],
                    "fromCity": record["fromCity"],
                    "toAirport": record["toAirport"],
                    "toCity": record["toCity"],
                    "price": record["price"],
                    "flightTimeInMinutes": record["flightTimeInMinutes"],
                    "operator": record["operator"]
                }
                return (flight_info), 200
            else:
                return "Flight not found", 404

    
    @app.route('/search/flights/<fromCity>/<toCity>', methods=['GET'])
    def search_flights(fromCity, toCity):
        with driver.session() as session:
        # Query to find direct or multi-stop flights between cities
            result = session.run(
                """
                MATCH path = (fromCity:City {name: $fromCity})-[:HAS_AIRPORT]->(fromAirport:Airport)
                            -[:FLIGHT_TO|ARRIVES_AT*..6]->(f:Flight)-[:ARRIVES_AT]->(toAirport:Airport)
                            <-[:HAS_AIRPORT]-(toCity:City {name: $toCity})
                WITH 
                    path,
                    fromAirport,
                    toAirport,
                    [node IN nodes(path) | node] AS pathNodes,
                    [node IN nodes(path) WHERE "Flight" IN labels(node) | COALESCE(node.price, 0)] AS flightPrices,
                    [node IN nodes(path) WHERE "Flight" IN labels(node) | COALESCE(node.flightTimeInMinutes, 0)] AS flightTimes
                RETURN 
                    fromAirport.code AS fromAirport,
                    toAirport.code AS toAirport,
                    nodes(path) AS flights,
                    [node IN pathNodes | node.code] AS airportCodes,
                    reduce(totalPrice = 0, price IN flightPrices | totalPrice + price) AS totalPrice,
                    reduce(totalTime = 0, time IN flightTimes | totalTime + time) AS totalTime
                ORDER BY totalPrice ASC
                LIMIT 10;

                """,
                fromCity=fromCity,
                toCity=toCity
            )

        # Process and format the results
            flights = []
            for record in result:
                if record:
                    flights.append({
                        "fromAirport": record["fromAirport"],
                        "toAirport": record["toAirport"],
                        "flights": [
                            node["number"] for node in record["flights"] if node.labels and "Flight" in node.labels
                        ],
                        "price": record["totalPrice"],
                        "flightTimeInMinutes": record["totalTime"]
                    })
            
            if flights:
                return (flights), 200
            else:
                return "Flights not found", 404


    
    @app.route('/cleanup', methods=['POST'])
    def cleanup():
        with driver.session() as session:
            session.run('''
            MATCH (n) DETACH DELETE n
            ''')

            return 'Cleaned up', 200

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)