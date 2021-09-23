SELECT SUM(CatchedPokemon.level)
FROM CatchedPokemon
JOIN Pokemon ON Pokemon.type = 'Fire' AND CatchedPokemon.pid = Pokemon.id;
