SELECT Trainer.name, AVG(CatchedPokemon.level)
FROM Trainer
JOIN Gym ON Trainer.id = Gym.leader_id
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id
GROUP BY Trainer.id, Trainer.name
ORDER BY Trainer.name;
