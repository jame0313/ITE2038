SELECT Pokemon.type, COUNT(*)
FROM CatchedPokemon
JOIN Pokemon ON CatchedPokemon.id = Pokemon.id
GROUP BY Pokemon.type
ORDER BY Pokemon.type
