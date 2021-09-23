SELECT CatchedPokemon.nickname
FROM CatchedPokemon
JOIN Trainer ON CatchedPokemon.level >= 40 AND Trainer.id >= 5 AND Trainer.id = CatchedPokemon.owner_id
ORDER BY CatchedPokemon.nickname;
