import os
import unittest
import json
import configparser
from pathalchemy import PathAlchemy


class TestQ(unittest.TestCase):
    def test_q(self):
        config = configparser.ConfigParser()
        config.read("test_config.ini")
        username = config.get("unittest", "username")
        password = config.get("unittest", "password")
        database = config.get("unittest", "database")
        driver = config.get("unittest", "driver")
        address = config.get("unittest", "address")
        port = config.get("unittest", "port")
        db = PathAlchemy.create(username,password,database,driver,address,port)
        encoder = json.JSONEncoder(ensure_ascii=False, separators=(",", ":"))
        for a, b, c in self.q_data():
            self.assertEqual(encoder.encode(db.path_query(a, b)), c)

    def q_data(self):
        return [
            [
                "select id, content from posts where id=:id",
                {"id": 1},
                '[{"id":1,"content":"blog started"}]',
            ],
            ["select id from posts where id<=2 order by id", {}, '[{"id":1},{"id":2}]'],
            [
                "select id from posts where id<=:two and id>=:one order by id",
                {"one": 1, "two": 2},
                '[{"id":1},{"id":2}]',
            ],
            [
                'select posts.id as "$[].posts.id", comments.id as "$[].comments.id" from posts left join comments on post_id = posts.id where posts.id=1 order by posts.id, comments.id',
                {},
                '[{"posts":{"id":1},"comments":{"id":1}},{"posts":{"id":1},"comments":{"id":2}}]',
            ],
            [
                'select posts.id as "$.posts[].id", comments.id as "$.posts[].comments[].id" from posts left join comments on post_id = posts.id where posts.id<=2 order by posts.id, comments.id',
                {},
                '{"posts":[{"id":1,"comments":[{"id":1},{"id":2}]},{"id":2,"comments":[{"id":3},{"id":4},{"id":5},{"id":6}]}]}',
            ],
            [
                'select posts.id as "$.comments[].post.id", comments.id as "$.comments[].id" from posts left join comments on post_id = posts.id where posts.id<=2 order by posts.id, comments.id',
                {},
                '{"comments":[{"id":1,"post":{"id":1}},{"id":2,"post":{"id":1}},{"id":3,"post":{"id":2}},{"id":4,"post":{"id":2}},{"id":5,"post":{"id":2}},{"id":6,"post":{"id":2}}]}',
            ],
            ['select count(*) as "posts" from posts', {}, '[{"posts":12}]'],
            ['select count(*) as "$[].posts" from posts', {}, '[{"posts":12}]'],
            ['select count(*) as "$.posts" from posts', {}, '{"posts":12}'],
            [
                'select categories.name, count(posts.id) as "post_count" from posts, categories where posts.category_id = categories.id group by categories.name order by categories.name',
                {},
                '[{"name":"announcement","post_count":11},{"name":"article","post_count":1}]',
            ],
            [
                'select count(*) as "$.statistics.posts" from posts',
                {},
                '{"statistics":{"posts":12}}',
            ],
            [
                'select (select count(*) from posts) as "$.stats.posts", (select count(*) from comments) as "comments"',
                {},
                '{"stats":{"posts":12,"comments":6}}',
            ],
        ]
