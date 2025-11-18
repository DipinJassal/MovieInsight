"""
MongoDB Handler for TMDB Data
Handles all MongoDB operations
"""

from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


class MongoHandler:
    """
    Handler for MongoDB operations
    """
    
    def __init__(self):
        mongo_uri = os.getenv('MONGO_URI')
        db_name = os.getenv('MONGO_DB_NAME')
        
        if not mongo_uri:
            raise ValueError("MONGO_URI not found in .env")
        
        if not db_name:
            raise ValueError("MONGO_DB_NAME not found in .env")
        
        try:
            self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client[db_name]
            print(f"✓ Connected to MongoDB: {db_name}")
        except Exception as e:
            raise ConnectionError(f"MongoDB connection failed: {str(e)}")
    
    def insert_movies(self, movies):
        """Insert or update movies"""
        if not movies:
            print("No movies to insert")
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        print(f"Inserting {len(movies)} movies...")
        
        timestamp = datetime.utcnow()
        for movie in movies:
            movie['loaded_at'] = timestamp
        
        operations = []
        for movie in movies:
            operations.append(
                UpdateOne(
                    {'id': movie['id']},
                    {'$set': movie},
                    upsert=True
                )
            )
        
        try:
            result = self.db.raw_movies.bulk_write(operations, ordered=False)
            inserted = result.upserted_count
            updated = result.modified_count
            
            print(f"  ✓ Inserted: {inserted}, Updated: {updated}")
            
            return {
                'inserted': inserted,
                'updated': updated,
                'errors': 0
            }
        except BulkWriteError as e:
            write_errors = e.details.get('writeErrors', [])
            inserted = e.details.get('nUpserted', 0)
            updated = e.details.get('nModified', 0)
            
            print(f"  ⚠ Inserted: {inserted}, Updated: {updated}, Errors: {len(write_errors)}")
            
            return {
                'inserted': inserted,
                'updated': updated,
                'errors': len(write_errors)
            }
        except Exception as e:
            print(f"  ✗ Error: {str(e)}")
            return {'inserted': 0, 'updated': 0, 'errors': len(movies)}
    
    def insert_credits(self, movie_id, credits):
        """Insert cast and crew credits"""
        if not credits:
            return {'cast_count': 0, 'crew_count': 0, 'errors': 0}
        
        timestamp = datetime.utcnow()
        cast_count = 0
        crew_count = 0
        errors = 0
        
        # Process cast
        for cast_member in credits.get('cast', []):
            try:
                cast_member['movie_id'] = movie_id
                cast_member['credit_type'] = 'cast'
                cast_member['loaded_at'] = timestamp
                
                if 'credit_id' in cast_member:
                    filter_query = {'credit_id': cast_member['credit_id']}
                else:
                    filter_query = {
                        'movie_id': movie_id,
                        'id': cast_member.get('id'),
                        'credit_type': 'cast'
                    }
                
                self.db.raw_credits.update_one(
                    filter_query,
                    {'$set': cast_member},
                    upsert=True
                )
                cast_count += 1
            except Exception as e:
                errors += 1
                continue
        
        # Process crew
        for crew_member in credits.get('crew', []):
            try:
                crew_member['movie_id'] = movie_id
                crew_member['credit_type'] = 'crew'
                crew_member['loaded_at'] = timestamp
                
                if 'credit_id' in crew_member:
                    filter_query = {'credit_id': crew_member['credit_id']}
                else:
                    filter_query = {
                        'movie_id': movie_id,
                        'id': crew_member.get('id'),
                        'job': crew_member.get('job'),
                        'credit_type': 'crew'
                    }
                
                self.db.raw_credits.update_one(
                    filter_query,
                    {'$set': crew_member},
                    upsert=True
                )
                crew_count += 1
            except Exception as e:
                errors += 1
                continue
        
        if cast_count > 0 or crew_count > 0:
            print(f"  ✓ Movie {movie_id}: {cast_count} cast, {crew_count} crew")
        
        return {
            'cast_count': cast_count,
            'crew_count': crew_count,
            'errors': errors
        }
    
    def insert_genres(self, genres):
        """Insert genres"""
        if not genres:
            print("No genres to insert")
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        print(f"Inserting {len(genres)} genres...")
        
        timestamp = datetime.utcnow()
        for genre in genres:
            genre['loaded_at'] = timestamp
        
        operations = []
        for genre in genres:
            operations.append(
                UpdateOne(
                    {'id': genre['id']},
                    {'$set': genre},
                    upsert=True
                )
            )
        
        try:
            result = self.db.raw_genres.bulk_write(operations, ordered=False)
            inserted = result.upserted_count
            updated = result.modified_count
            
            print(f"  ✓ Inserted: {inserted}, Updated: {updated}")
            
            return {
                'inserted': inserted,
                'updated': updated,
                'errors': 0
            }
        except BulkWriteError as e:
            write_errors = e.details.get('writeErrors', [])
            inserted = e.details.get('nUpserted', 0)
            updated = e.details.get('nModified', 0)
            
            return {
                'inserted': inserted,
                'updated': updated,
                'errors': len(write_errors)
            }
        except Exception as e:
            print(f"  ✗ Error: {str(e)}")
            return {'inserted': 0, 'updated': 0, 'errors': len(genres)}
    
    def insert_companies(self, companies):
        """Insert production companies"""
        if not companies:
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        valid_companies = [c for c in companies if c.get('id')]
        
        if not valid_companies:
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        timestamp = datetime.utcnow()
        for company in valid_companies:
            company['loaded_at'] = timestamp
        
        operations = []
        for company in valid_companies:
            operations.append(
                UpdateOne(
                    {'id': company['id']},
                    {'$set': company},
                    upsert=True
                )
            )
        
        try:
            result = self.db.raw_companies.bulk_write(operations, ordered=False)
            inserted = result.upserted_count
            updated = result.modified_count
            
            if inserted > 0 or updated > 0:
                print(f"  ✓ Companies: {inserted} new, {updated} updated")
            
            return {
                'inserted': inserted,
                'updated': updated,
                'errors': 0
            }
        except BulkWriteError as e:
            write_errors = e.details.get('writeErrors', [])
            inserted = e.details.get('nUpserted', 0)
            updated = e.details.get('nModified', 0)
            
            return {
                'inserted': inserted,
                'updated': updated,
                'errors': len(write_errors)
            }
        except Exception as e:
            print(f"  ✗ Error: {str(e)}")
            return {'inserted': 0, 'updated': 0, 'errors': len(valid_companies)}
    
    def get_document_count(self, collection_name):
        """Get count of documents in collection"""
        try:
            collection = self.db[collection_name]
            count = collection.count_documents({})
            return count
        except Exception as e:
            print(f"Error counting documents: {str(e)}")
            return 0
    
    def get_stats(self):
        """Get statistics about all collections"""
        stats = {
            'movies': self.get_document_count('raw_movies'),
            'credits': self.get_document_count('raw_credits'),
            'genres': self.get_document_count('raw_genres'),
            'companies': self.get_document_count('raw_companies')
        }
        return stats
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            print("✓ MongoDB connection closed")


# Test code
if __name__ == "__main__":
    print("Testing MongoDB Handler...")
    
    try:
        handler = MongoHandler()
        
        # Test genres
        test_genres = [
            {'id': 99991, 'name': 'Test Genre 1'},
            {'id': 99992, 'name': 'Test Genre 2'}
        ]
        genre_stats = handler.insert_genres(test_genres)
        print(f"Genre stats: {genre_stats}")
        
        # Test movies
        test_movies = [
            {
                'id': 999991,
                'title': 'Test Movie',
                'release_date': '2024-01-01',
                'vote_average': 8.5
            }
        ]
        movie_stats = handler.insert_movies(test_movies)
        print(f"Movie stats: {movie_stats}")
        
        # Get stats
        stats = handler.get_stats()
        print(f"Database stats: {stats}")
        
        # Cleanup
        handler.db.raw_movies.delete_many({'id': {'$gte': 999990}})
        handler.db.raw_genres.delete_many({'id': {'$gte': 99990}})
        
        handler.close()
        
        print("\n✅ MongoDB Handler test passed!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")