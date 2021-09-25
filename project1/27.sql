SELECT CatchedPokemon.nickname
FROM CatchedPokemon
WHERE CatchedPokemon.level >= 40 AND CatchedPokemon.owner_id >= 5
ORDER BY CatchedPokemon.nickname;