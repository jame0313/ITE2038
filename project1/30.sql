;
SELECT Pokemon.name
FROM Pokemon
WHERE Pokemon.name LIKE '%s' OR Pokemon.name LIKE '%S'
ORDER BY Pokemon.name
