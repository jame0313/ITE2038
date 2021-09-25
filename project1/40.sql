SELECT Trainer.name
From Trainer
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id
JOIN Pokemon ON Pokemon.name = 'Pikachu' AND CatchedPokemon.pid = Pokemon.id
GROUP BY Trainer.id, Trainer.name
ORDER BY Trainer.name;