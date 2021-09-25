SELECT Trainer.name
FROM Trainer
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id
JOIN Pokemon ON Pokemon.type ='Psychic' AND CatchedPokemon.pid = Pokemon.id
GROUP BY Trainer.id, Trainer.name
ORDER BY Trainer.name;