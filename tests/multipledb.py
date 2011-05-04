import unittest
from datetime import datetime
import pymongo

from mongoengine import *
from mongoengine.connection import connect, _get_db


class MultipleDatabaseTest(unittest.TestCase):
    
    def setUp(self):
        connect(db='mongoenginetest')
        other_db = pymongo.Connection()['mongoenginetest_other']

        class Person(Document):
            name = StringField()
            age = IntField()
            meta = {
              'indexes': ['name'],
              'create_indexes': True,
            }

        class Alien(Document):
            name = StringField()
            galaxy = StringField()
            has_mass = BooleanField()
            meta = {
              'indexes': ['galaxy', 'has_mass'],
              'create_indexes': True,
            }

            @classmethod
            def _get_db(cls):
              return other_db

        self.Person = Person
        self.Alien = Alien
        self.db = _get_db()
        self.other_db = other_db

    def test_drop_collection(self):
        """Ensure that the collection may be dropped from the database.
        """
        person_col = self.Person._meta['collection']
        alien_col = self.Alien._meta['collection']

        def assertExistence(person, alien):
          self.assertTrue(person == (person_col in self.db.collection_names()))
          self.assertFalse(person_col in self.other_db.collection_names())
          self.assertTrue(alien == (alien_col in self.other_db.collection_names()))
          self.assertFalse(alien_col in self.db.collection_names())

        assertExistence(False, False)
        self.Person(name='Test').save()
        self.Alien(name='Test').save()
        assertExistence(True, True)
        self.Person.drop_collection()
        assertExistence(False, True)
        self.Alien.drop_collection()
        assertExistence(False, False)
        self.Alien(name='Test').save()
        assertExistence(False, True)

    def test_indexes(self):
        """Ensure that indexes are used when meta[indexes] is specified.
        """
        info = self.Alien.objects._collection.index_information()
        print info
        self.assertEqual(len(info), 3)
        info = self.Person.objects._collection.index_information()
        print info
        self.assertEqual(len(info), 2)

    def test_save(self):
        """Ensure that a document may be saved in the database.
        """
        person = self.Person(name='Test User', age=30)
        person.save()
        alien = self.Alien(name='Test Alien', galaxy='far away')
        alien.save()
        # Ensure that the objects are in the right databases.
        collection = self.db[self.Person._meta['collection']]
        person_obj = collection.find_one({'name': 'Test User'})
        self.assertEqual(0, self.db[self.Alien._meta['collection']].count())
        self.assertEqual(person_obj['name'], 'Test User')
        self.assertEqual(person_obj['age'], 30)
        self.assertEqual(person_obj['_id'], person.id)
        collection = self.other_db[self.Alien._meta['collection']]
        alien_obj = collection.find_one({'name': 'Test Alien'})
        self.assertEqual(0, self.other_db[self.Person._meta['collection']].count())
        self.assertEqual(alien_obj['name'], 'Test Alien')
        self.assertEqual(alien_obj['galaxy'], 'far away')
        self.assertEqual(alien_obj['_id'], alien.id)


    def test_delete(self):
        """Ensure that document may be deleted using the delete method.
        """
        person = self.Person(name="Test User", age=30)
        person.save()
        alien = self.Alien(name='Test Alien', galaxy='far away')
        alien.save()
        self.assertEqual(len(self.Person.objects), 1)
        self.assertEqual(len(self.Alien.objects), 1)
        alien.delete()
        person.delete()
        self.assertEqual(len(self.Person.objects), 0)
        self.assertEqual(len(self.Alien.objects), 0)

    def tearDown(self):
        self.Alien.drop_collection()
        self.Person.drop_collection()


if __name__ == '__main__':
    unittest.main()
