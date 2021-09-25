SELECT Pokemon.name
FROM Pokemon
WHERE LEFT(Pokemon.name,1) IN ('A','E','I','O','U','a','e','i','o','u')
ORDER BY Pokemon.name;