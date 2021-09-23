;
SELECT COUNT(*)
FROM CatchedPokemon
JOIN Pokemon ON CatchedPokemon.pid = Pokemon.id
GROUP BY Pokemon.type
ORDER BY Pokemon.type;
