SELECT Trainer.name
FROM Trainer
LEFT OUTER JOIN Gym ON Trainer.id = Gym.leader_id
WHERE Gym.leader_id IS NULL
ORDER BY Trainer.name
