SELECT Trainer.name
FROM Gym
JOIN Trainer ON Gym.leader_id = Trainer.id
JOIN City ON City.name = Gym.city
WHERE City.description = 'Amazon';
