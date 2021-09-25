SELECT CatchedPokemon.nickname
FROM Gym
JOIN CatchedPokemon ON Gym.city = 'Sangnok City' AND Gym.leader_id = CatchedPokemon.owner_id
JOIN Pokemon ON CatchedPokemon.pid = Pokemon.id AND Pokemon.type = 'Water'
ORDER BY CatchedPokemon.nickname;