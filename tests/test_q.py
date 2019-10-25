import os
import unittest
from pathalchemy import PathAlchemy
from json import JSONEncoder

class TestQ(unittest.TestCase):
    def test_q(self):
        p = PathAlchemy.create(os.environ["SQLALCHEMY_USERNAME"],os.environ["SQLALCHEMY_PASSWORD"],os.environ["SQLALCHEMY_DATABASE"])
        encoder = JSONEncoder(ensure_ascii=False,separators=(',',':'))
        for a,b,c in self.q_data():
            self.assertEqual(encoder.encode(p.q(a,b)), c)

    def q_data(self):
        return [
            ["""select posts.id as "$.posts[].id", comments.id as "$.posts[].comments[].id" from posts left join comments on post_id = posts.id where posts.id<=2 order by posts.id, comments.id""", {},
            '{"posts":[{"id":1,"comments":[{"id":1},{"id":2}]},{"id":2,"comments":[{"id":3},{"id":4},{"id":5},{"id":6}]}]}'],
        ]