;
SELECT CatchedPokemon.nickname
FROM Trainer
JOIN Gym ON Gym.city = 'Sangnok CIty' AND Trainer.id = Gym.leader_id
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id
JOIN Pokemon ON CatchedPokemon.pid = Pokemon.id
WHERE Pokemon.type = 'water'
ORDER BY CatchedPokemon.nickname
