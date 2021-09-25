SELECT Trainer.name
FROM Gym
JOIN City ON City.description = 'Amazon' AND City.name = Gym.city
JOIN Trainer ON Gym.leader_id = Trainer.id;