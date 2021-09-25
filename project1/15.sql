SELECT Trainer.name
From Pokemon
JOIN CatchedPokemon ON Pokemon.name LIKE 'P%' AND Pokemon.id = CatchedPokemon.pid
JOIN Trainer ON Trainer.hometown = 'Sangnok City' AND Trainer.id = CatchedPokemon.owner_id
GROUP BY Trainer.id, Trainer.name
ORDER BY Trainer.name;