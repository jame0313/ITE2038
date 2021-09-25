SELECT Pokemon.name
From Gym
JOIN Trainer ON Gym.city = 'Rainbow City' AND Trainer.id = Gym.leader_id
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id
JOIN Pokemon ON CatchedPokemon.pid = Pokemon.id
GROUP BY Pokemon.id, Pokemon.name
ORDER BY Pokemon.name;