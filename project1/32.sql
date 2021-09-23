SELECT OneTrainer.name, Pokemon.name, COUNT(*)
FROM
(SELECT Trainer.id AS id, Trainer.name AS name
FROM Trainer
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id
JOIN Pokemon ON CatchedPokemon.pid = Pokemon.id
GROUP BY Trainer.id, Trainer.name
HAVING COUNT(DISTINCT Pokemon.type) = 1
) AS OneTrainer 
JOIN CatchedPokemon ON OneTrainer.id = CatchedPokemon.owner_id
JOIN Pokemon ON CatchedPokemon.pid = Pokemon.id
GROUP BY OneTrainer.id, OneTrainer.name, Pokemon.id, Pokemon.name
ORDER BY OneTrainer.name
