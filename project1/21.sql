SELECT Duplicated_Trainer.name
FROM (SELECT DISTINCT Trainer.id AS id, Trainer.name AS name
FROM Trainer
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id
GROUP BY Trainer.id, Trainer.name, CatchedPokemon.pid
HAVING COUNT(*) >= 2) AS Duplicated_Trainer
ORDER BY Duplicated_Trainer.name;