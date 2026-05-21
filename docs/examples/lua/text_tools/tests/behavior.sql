CREATE TABLE words(name TEXT);
INSERT INTO words VALUES ('hello'), ('decent'), ('database');

SELECT slugify('Hello, DecentDB');
SELECT word FROM split_words('a bb c');
SELECT lua_sum(length(name)) FROM words;
SELECT name FROM words ORDER BY name COLLATE reverse_text;
