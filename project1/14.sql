SELECT City.name
FROM City
JOIN Trainer ON City.name = Trainer.hometown
GROUP BY City.name
HAVING COUNT(*)>=ALL(SELECT COUNT(*)
FROM City
JOIN Trainer ON City.name = Trainer.hometown
GROUP BY City.name);
