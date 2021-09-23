SELECT Trainer.name
From Gym
JOIN Trainer ON Gym.city = 'Brown City' AND Trainer.id = Gym.leader_id
