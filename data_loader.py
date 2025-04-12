import pyarrow.parquet as pq
import pandas as pd
from neo4j import GraphDatabase
import time

class DataLoader:

    def __init__(self, uri, user, password):
        """
        Connect to the Neo4j database and other init steps

        Args:
            uri (str): URI of the Neo4j database
            user (str): Username of the Neo4j database
            password (str): Password of the Neo4j database
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self.driver.verify_connectivity()

    def close(self):
        """
        Close the connection to the Neo4j database
        """
        self.driver.close()

    def create_location_node(self, session, location_id):
        """
        Creates a Location node in Neo4j if it does not already exist.
        
        Args:
            session (neo4j.Session): Neo4j session object
            location_id (int): Location ID to create as a node
        """
        session.run("""
            MERGE (l:Location {name: $location_id})
        """, location_id=location_id)

    def create_trip_relationship(self, session, trip):
        """
        Creates a TRIP relationship in Neo4j between two Location nodes.
        
        Args:
            session (neo4j.Session): Neo4j session object
            trip (dict): Trip data containing pickup/dropoff locations and trip attributes
        """
        session.run("""
            MATCH (start:Location {name: $PULocationID}), (end:Location {name: $DOLocationID})
            MERGE (start)-[:TRIP {
                distance: $trip_distance,
                fare: $fare_amount,
                pickup_dt: $tpep_pickup_datetime,
                dropoff_dt: $tpep_dropoff_datetime
            }]->(end)
        """, **trip)

    def load_transform_file(self, file_path):
        """
        Load the parquet file, filter and clean the data, create nodes and relationships in Neo4j.
        
        Args:
            file_path (str): Path to the parquet file to be loaded
        """
        trips = pq.read_table(file_path).to_pandas()

        # Select necessary columns and filter data
        trips = trips[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'trip_distance', 'fare_amount']]
        bronx_locations = set([3, 18, 20, 31, 32, 46, 47, 51, 58, 59, 60, 69, 78, 81, 94, 119, 126, 136, 147, 159, 167, 168, 169, 174, 182, 183, 184, 185, 199, 200, 208, 212, 213, 220, 235, 240, 241, 242, 247, 248, 250, 254, 259])
        
        # Filter to include only trips starting and ending in the Bronx with valid distance and fare
        trips = trips[(trips['PULocationID'].isin(bronx_locations)) & 
                      (trips['DOLocationID'].isin(bronx_locations)) & 
                      (trips['trip_distance'] > 0.1) & 
                      (trips['fare_amount'] > 2.5)]
        
        # Convert date columns to datetime format
        trips['tpep_pickup_datetime'] = pd.to_datetime(trips['tpep_pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
        trips['tpep_dropoff_datetime'] = pd.to_datetime(trips['tpep_dropoff_datetime'], format='%Y-%m-%d %H:%M:%S')

        # Connect to Neo4j and load the data
        with self.driver.session() as session:
            # Create nodes for unique pickup and dropoff locations
            for location_id in pd.concat([trips['PULocationID'], trips['DOLocationID']]).unique():
                self.create_location_node(session, location_id)
            
            # Create TRIP relationships with trip details
            for _, row in trips.iterrows():
                trip = {
                    "PULocationID": int(row['PULocationID']),
                    "DOLocationID": int(row['DOLocationID']),
                    "trip_distance": float(row['trip_distance']),
                    "fare_amount": float(row['fare_amount']),
                    "tpep_pickup_datetime": row['tpep_pickup_datetime'].to_pydatetime(),
                    "tpep_dropoff_datetime": row['tpep_dropoff_datetime'].to_pydatetime()
                }
                self.create_trip_relationship(session, trip)

def main():
    total_attempts = 10
    attempt = 0

    # Retry connecting to the database
    while attempt < total_attempts:
        try:
            data_loader = DataLoader("neo4j://localhost:7687", "neo4j", "project1phase1")
            data_loader.load_transform_file("/cse511/yellow_tripdata_2022-03.parquet")
            data_loader.close()
            break
        except Exception as e:
            print(f"(Attempt {attempt+1}/{total_attempts}) Error:", e)
            attempt += 1
            time.sleep(10)

if __name__ == "__main__":
    main()
