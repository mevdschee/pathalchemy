import unittest
from .pathql import PathAlchemy
from json import JSONEncoder

class DefaultWidgetSizeTestCase(unittest.TestCase):
    def test_q(self):
        p = PathAlchemy.create('php-crud-api','php-crud-api','php-crud-api')
        results = p.q("""select posts.id as "$.posts[].id", comments.id as "$.posts[].comments[].id" from posts left join comments on post_id = posts.id where posts.id<=2 order by posts.id, comments.id""")
        self.assertEqual(JSONEncoder().encode(results), '{"posts": [{"id": 1, "comments": [{"id": 1}, {"id": 2}]}, {"id": 2, "comments": [{"id": 3}, {"id": 4}, {"id": 5}, {"id": 6}]}]}')
