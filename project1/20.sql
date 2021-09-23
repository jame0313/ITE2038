SELECT Trainer.name
FROM Gym
JOIN Trainer ON Trainer.id = Gym.leader_id
ORDER BY Trainer.name;
