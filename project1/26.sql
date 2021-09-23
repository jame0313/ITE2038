SELECT Trainer.name, SUM(CatchedPokemon.level) AS TotalLevel
FROM Trainer
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id
GROUP BY Trainer.id, Trainer.name
HAVING SUM(CatchedPokemon.level) >= ALL(
SELECT SUM(CatchedPokemon.level) AS totalLevel
FROM Trainer
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id
GROUP BY Trainer.id, Trainer.name
ORDER BY totalLevel DESC)
