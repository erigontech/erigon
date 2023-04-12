/* Student Name: First LAST */
/* N number: 12345678 */

DROP DATABASE IF EXISTS `hw03`;

CREATE DATABASE `hw03`; 
USE `hw03`;


/* Clean up any previous homework answers */
DROP TABLE IF EXISTS ANSWER00;
DROP TABLE IF EXISTS ANSWER01;
DROP TABLE IF EXISTS ANSWER02;
DROP TABLE IF EXISTS ANSWER03;
DROP TABLE IF EXISTS ANSWER04;
DROP TABLE IF EXISTS ANSWER05;
DROP TABLE IF EXISTS ANSWER06;
DROP TABLE IF EXISTS ANSWER07;
DROP TABLE IF EXISTS ANSWER08;
DROP TABLE IF EXISTS ANSWER09;
DROP TABLE IF EXISTS TEMP0;
DROP TABLE IF EXISTS TEMP1;
DROP TABLE IF EXISTS TEMP2;
DROP TABLE IF EXISTS TEMP3;
DROP TABLE IF EXISTS TEMP4;
DROP TABLE IF EXISTS TEMP5;
DROP TABLE IF EXISTS TEMP6;
DROP TABLE IF EXISTS TEMP7;
DROP TABLE IF EXISTS TEMP8;
DROP TABLE IF EXISTS TEMP9;

--
-- Table structure for table `adult`
--
/* Clean up any previous version */
DROP TABLE IF EXISTS `book`;
DROP TABLE IF EXISTS `clean`;
DROP TABLE IF EXISTS `orders`;
DROP TABLE IF EXISTS `double_room`;
DROP TABLE IF EXISTS `single_room`;
DROP TABLE IF EXISTS `room`;
DROP TABLE IF EXISTS `cook`;
DROP TABLE IF EXISTS `receptionist`;
DROP TABLE IF EXISTS `housekeeper`;
DROP TABLE IF EXISTS `employee`;
DROP TABLE IF EXISTS `child`;
DROP TABLE IF EXISTS `adult`;
DROP TABLE IF EXISTS `guest`;
DROP TABLE IF EXISTS `meal`;


/* Create the tables */


CREATE TABLE `employee` (
  `ID` int(11) NOT NULL,  
  `SSN` int(11) NOT NULL,
  `Name` varchar(45) DEFAULT NULL,
  `DOB` date DEFAULT NULL,
  `CivilStatus` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `cook` (
  `cook_ID` int(11) NOT NULL,
  `Speciality` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`cook_ID`),
  CONSTRAINT `cook_ID` FOREIGN KEY (`cook_ID`) REFERENCES `employee` (`ID`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `receptionist` (
  `receptionist_ID` int(11) NOT NULL,
  `Language` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`receptionist_ID`),
  CONSTRAINT `receptionist_ID` FOREIGN KEY (`receptionist_ID`) REFERENCES `employee` (`ID`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `housekeeper` (
  `housekeeper_ID` int(11) NOT NULL,
  PRIMARY KEY (`housekeeper_ID`),
  CONSTRAINT `housekeeper_ID` FOREIGN KEY (`housekeeper_ID`) REFERENCES `employee` (`ID`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `guest` (
  `ID` int(11) NOT NULL,
  `FName` varchar(45) DEFAULT NULL,
  `LName` varchar(45) DEFAULT NULL,
  `accompany_ID` int(11) DEFAULT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `accompany_ID_UNIQUE` (`accompany_ID`),
  KEY `accompany_ID_idx` (`accompany_ID`),
  CONSTRAINT `accompany_ID` FOREIGN KEY (`accompany_ID`) REFERENCES `guest` (`ID`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `adult` (
  `adult_ID` int(11) NOT NULL,
  `Age` int(11) NOT NULL,
  PRIMARY KEY (`adult_ID`),
  CONSTRAINT `adult_ID` FOREIGN KEY (`adult_ID`) REFERENCES `guest` (`ID`) ON DELETE CASCADE 
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `child` (
  `child_ID` int(11) NOT NULL,
  `adult_ID1` int(11) NOT NULL,
  `adult_ID2` int(11) DEFAULT NULL,
  `Age` int(11) DEFAULT NULL,
  PRIMARY KEY (`child_ID`,`adult_ID1`),
  KEY `adult_ID1_idx` (`adult_ID1`),
  KEY `adult_ID2_idx` (`adult_ID2`),
  CONSTRAINT `adult_ID1` FOREIGN KEY (`adult_ID1`) REFERENCES `adult` (`adult_ID`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `adult_ID2` FOREIGN KEY (`adult_ID2`) REFERENCES `adult` (`adult_ID`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `child_ID` FOREIGN KEY (`child_ID`) REFERENCES `guest` (`ID`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `room` (
  `ID` int(11) NOT NULL,
  `Number` varchar(45) DEFAULT NULL,
  `View` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `single_room` (
  `single_ID` int(11) NOT NULL,
  `Size` int(11) NOT NULL,  
  PRIMARY KEY (`single_ID`),
  CONSTRAINT `single_ID` FOREIGN KEY (`single_ID`) REFERENCES `room` (`ID`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `double_room` (
  `double_ID` int(11) NOT NULL,
  `BedType` varchar(45) DEFAULT NULL,  
  PRIMARY KEY (`double_ID`),
  CONSTRAINT `double_ID` FOREIGN KEY (`double_ID`) REFERENCES `room` (`ID`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `booking` (
  `ID` int(11) NOT NULL,  
  `guest_ID` int(11) NOT NULL,
  `room_ID` int(11) NOT NULL,
  `receptionist_ID` int(11) NOT NULL, 
  `From` date NOT NULL,
  `To` date NOT NULL, 
  `Price` int(11) DEFAULT NULL,
  `Payment_method` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`ID`),
  CONSTRAINT `guest_ID` FOREIGN KEY (`guest_ID`) REFERENCES `guest` (`ID`),
  CONSTRAINT `room_ID` FOREIGN KEY (`room_ID`) REFERENCES `room` (`ID`),
  CONSTRAINT `booking_receptionist_ID` FOREIGN KEY (`receptionist_ID`) REFERENCES `receptionist` (`receptionist_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `meal` (
  `ID` int(11) NOT NULL,
  `Title` varchar(45) DEFAULT NULL,
  `Type` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `orders` (
  `ID` int(11) NOT NULL,
  `guest_ID` int(11) NOT NULL,
  `meal_ID` int(11) NOT NULL,
  `cook_ID` int(11) NOT NULL, 
  PRIMARY KEY (`ID`),
  CONSTRAINT `order_guest_ID` FOREIGN KEY (`guest_ID`) REFERENCES `guest` (`ID`),
  CONSTRAINT `meal_ID` FOREIGN KEY (`meal_ID`) REFERENCES `meal` (`ID`),
  CONSTRAINT `order_cook_ID` FOREIGN KEY (`cook_ID`) REFERENCES `cook` (`cook_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `clean` (
  `ID` int(11) NOT NULL,
  `housekeeper_ID` int(11) NOT NULL,
  `room_ID` int(11) NOT NULL,
  `Date` date NOT NULL, 
  PRIMARY KEY (`ID`),
  CONSTRAINT `clean_room_ID` FOREIGN KEY (`room_ID`) REFERENCES `room` (`ID`),
  CONSTRAINT `clean_housekeeper_ID` FOREIGN KEY (`housekeeper_ID`) REFERENCES `housekeeper` (`housekeeper_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `employee`
--

LOCK TABLES `employee` WRITE;
INSERT INTO `employee` VALUES (1,1000,'Andy','1990-01-01','single');
INSERT INTO `employee` VALUES (2,1001,'Pablo','1980-02-01','married');
INSERT INTO `employee` VALUES (3,1002,'Vincent','1985-01-02','married');
INSERT INTO `employee` VALUES (4,1003,'Henry','1975-12-01','married');
INSERT INTO `employee` VALUES (5,1004,'Jackson','1965-11-01','divorced');
INSERT INTO `employee` VALUES (6,1005,'Edward','1993-12-02','single');
INSERT INTO `employee` VALUES (7,1006,'Leonardo','1988-12-12','married');
INSERT INTO `employee` VALUES (8,1007,'Salvador','1987-01-02','divorced');
INSERT INTO `employee` VALUES (9,1008,'Frida','1985-05-04','single');
INSERT INTO `employee` VALUES (10,1009,'Yayoyi','1979-04-02','married');
INSERT INTO `employee` VALUES (11,1010,'Alessandra','1960-11-11','single');
INSERT INTO `employee` VALUES (12,1011,NULL,NULL,'married');
INSERT INTO `employee` VALUES (13,1012,NULL,'1960-11-11','single');
INSERT INTO `employee` VALUES (14,1013,'Lucy','1999-11-11','single');
INSERT INTO `employee` VALUES (15,1014,'Claudia','1970-08-11','single');
INSERT INTO `employee` VALUES (16,1015,'Pamela','1986-07-11','married');
INSERT INTO `employee` VALUES (17,1016,'Alessio','1980-11-07','married');
INSERT INTO `employee` VALUES (18,1017,'Andrea','1961-11-11','single');
INSERT INTO `employee` VALUES (19,1018,'Ilia','2000-08-17','single');
INSERT INTO `employee` VALUES (20,1019,'Samanta','1960-11-11','single');
INSERT INTO `employee` VALUES (21,1020,NULL,'1960-04-11','married');
UNLOCK TABLES;

--
-- Dumping data for table `cook`
--
LOCK TABLES `cook` WRITE;
INSERT INTO `cook` VALUES (2,'pastery');
INSERT INTO `cook` VALUES (14,'steak');
INSERT INTO `cook` VALUES (15,'pasta');
INSERT INTO `cook` VALUES (20,'vegan food');
INSERT INTO `cook` VALUES (21,'sea food');
INSERT INTO `cook` VALUES (11,'omlet');
INSERT INTO `cook` VALUES (18,'pizza');
UNLOCK TABLES;

--
-- Dumping data for table `receptionist`
--
LOCK TABLES `receptionist` WRITE;
INSERT INTO `receptionist` VALUES (4,'English');
INSERT INTO `receptionist` VALUES (13,'German');
INSERT INTO `receptionist` VALUES (16,'Italian');
INSERT INTO `receptionist` VALUES (17,'French');
INSERT INTO `receptionist` VALUES (19,'English');
UNLOCK TABLES;

--
-- Dumping data for table `housekeeper`
--

LOCK TABLES `housekeeper` WRITE;
INSERT INTO `housekeeper` VALUES (1);
INSERT INTO `housekeeper` VALUES (3);
INSERT INTO `housekeeper` VALUES (5);
INSERT INTO `housekeeper` VALUES (6);
INSERT INTO `housekeeper` VALUES (7);
INSERT INTO `housekeeper` VALUES (8);
INSERT INTO `housekeeper` VALUES (9);
INSERT INTO `housekeeper` VALUES (10);
INSERT INTO `housekeeper` VALUES (11);
UNLOCK TABLES;



--
-- Dumping data for table `guest`
--

LOCK TABLES `guest` WRITE;
INSERT INTO `guest` VALUES (1,'Sam','Davidson',NULL);
INSERT INTO `guest` VALUES (2,'John','Davidson',NULL);
INSERT INTO `guest` VALUES (3,'Andre','Cox',NULL);
INSERT INTO `guest` VALUES (4,'Ian','Costa',NULL);
INSERT INTO `guest` VALUES (5,'Roberto','Davis',NULL);
INSERT INTO `guest` VALUES (6,'Michael','Davidson',NULL);
INSERT INTO `guest` VALUES (7,'Laura','Weiss',NULL);
INSERT INTO `guest` VALUES (8,'Nargiz','Ginepri',NULL);
INSERT INTO `guest` VALUES (9,'Andy','Gudenzi',2);
INSERT INTO `guest` VALUES (10,'Adam','Sandler',3);
INSERT INTO `guest` VALUES (11,'Anastazia','Carry',4);
INSERT INTO `guest` VALUES (12,'Elena','Lopez',1);
INSERT INTO `guest` VALUES (13,'Mary','Dobrev',NULL);
INSERT INTO `guest` VALUES (14,'Julian','Kriek',NULL);
INSERT INTO `guest` VALUES (15,'Thomas','Gudenzi',5);
INSERT INTO `guest` VALUES (16,'Jay','Kulti',NULL);
INSERT INTO `guest` VALUES (17,'Daniel','McNeil',6);
INSERT INTO `guest` VALUES (18,'Andy','Muller',7);
INSERT INTO `guest` VALUES (19,'Elena','Murray',NULL);
INSERT INTO `guest` VALUES (20,'Elena','Cecil',8);
INSERT INTO `guest` VALUES (21,'Vincenzo','Cali',NULL);
UNLOCK TABLES;

--
-- Dumping data for table `adult`
--

LOCK TABLES `adult` WRITE;
INSERT INTO `adult` VALUES (2,30);
INSERT INTO `adult` VALUES (8,31);
INSERT INTO `adult` VALUES (10,22);
INSERT INTO `adult` VALUES (11,25);
INSERT INTO `adult` VALUES (12,24);
INSERT INTO `adult` VALUES (13,33);
INSERT INTO `adult` VALUES (14,31);
INSERT INTO `adult` VALUES (15,40);
INSERT INTO `adult` VALUES (16,45);
INSERT INTO `adult` VALUES (17,60);
INSERT INTO `adult` VALUES (18,30);
INSERT INTO `adult` VALUES (19,17);
INSERT INTO `adult` VALUES (20,16);
INSERT INTO `adult` VALUES (21,30);
UNLOCK TABLES;

--
-- Dumping data for table `child`
--

LOCK TABLES `child` WRITE;
INSERT INTO `child` VALUES (1,2,8,10);
INSERT INTO `child` VALUES (2,12,NULL,1);
INSERT INTO `child` VALUES (4,2,NULL,11);
INSERT INTO `child` VALUES (5,10,NULL,3);
INSERT INTO `child` VALUES (6,10,NULL,2);
INSERT INTO `child` VALUES (7,11,NULL,5);
INSERT INTO `child` VALUES (9,13,15,7);
UNLOCK TABLES;

--
-- Dumping data for table `room`
--

LOCK TABLES `room` WRITE;
INSERT INTO `room` VALUES (1,'101','stardard');
INSERT INTO `room` VALUES (2,'102','mountain view');
INSERT INTO `room` VALUES (3,'103','stardard');
INSERT INTO `room` VALUES (4,'104','stardard');
INSERT INTO `room` VALUES (5,'105','stardard');
INSERT INTO `room` VALUES (6,'106','lake view');
INSERT INTO `room` VALUES (7,'107','lake view');
INSERT INTO `room` VALUES (8,'108','mountain view');
INSERT INTO `room` VALUES (9,'109','lake view');
INSERT INTO `room` VALUES (10,'110','mountain view');
INSERT INTO `room` VALUES (11,'201','lake view');
INSERT INTO `room` VALUES (12,'202','lake view');
INSERT INTO `room` VALUES (13,'203','lake view');
INSERT INTO `room` VALUES (14,'204','stardard');
INSERT INTO `room` VALUES (15,'205','lake view');
INSERT INTO `room` VALUES (16,'206','lake view');
INSERT INTO `room` VALUES (17,'207','lake view');
INSERT INTO `room` VALUES (18,'208','lake view');
INSERT INTO `room` VALUES (19,'209','lake view');
INSERT INTO `room` VALUES (20,'210','mountain view');
INSERT INTO `room` VALUES (21,'301','lake view');
INSERT INTO `room` VALUES (22,'302','lake view');
INSERT INTO `room` VALUES (23,'303','mountain view');
INSERT INTO `room` VALUES (24,'304','lake view');
INSERT INTO `room` VALUES (25,'305','lake view');
INSERT INTO `room` VALUES (26,'306','stardard');
INSERT INTO `room` VALUES (27,'307','lake view');
INSERT INTO `room` VALUES (28,'308','stardard');
INSERT INTO `room` VALUES (29,'309','lake view');
INSERT INTO `room` VALUES (30,'310','mountain view');
UNLOCK TABLES;

--
-- Dumping data for table `single`
--

LOCK TABLES `single_room` WRITE;
INSERT INTO `single_room` VALUES (1,25);
INSERT INTO `single_room` VALUES (2,27);
INSERT INTO `single_room` VALUES (5,30);
INSERT INTO `single_room` VALUES (7,20);
INSERT INTO `single_room` VALUES (8,25);
INSERT INTO `single_room` VALUES (9,32);
INSERT INTO `single_room` VALUES (28,33);
INSERT INTO `single_room` VALUES (29,15);
INSERT INTO `single_room` VALUES (30,10);
UNLOCK TABLES;


--
-- Dumping data for table `double`
--

LOCK TABLES `double_room` WRITE;
INSERT INTO `double_room` VALUES (3,'twin');
INSERT INTO `double_room` VALUES (4,'king');
INSERT INTO `double_room` VALUES (6,'queen');
INSERT INTO `double_room` VALUES (10,'queen');
INSERT INTO `double_room` VALUES (11,'queen');
INSERT INTO `double_room` VALUES (12,'twin');
INSERT INTO `double_room` VALUES (13,'queen');
INSERT INTO `double_room` VALUES (14,'king');
INSERT INTO `double_room` VALUES (15,'queen');
INSERT INTO `double_room` VALUES (16,'queen');
INSERT INTO `double_room` VALUES (17,'twin');
INSERT INTO `double_room` VALUES (18,'king');
INSERT INTO `double_room` VALUES (19,'queen');
INSERT INTO `double_room` VALUES (20,'queen');
INSERT INTO `double_room` VALUES (21,'twin');
INSERT INTO `double_room` VALUES (22,'queen');
INSERT INTO `double_room` VALUES (23,'king');
INSERT INTO `double_room` VALUES (24,'queen');
INSERT INTO `double_room` VALUES (25,'king');
INSERT INTO `double_room` VALUES (26,'queen');
INSERT INTO `double_room` VALUES (27,'twin');
UNLOCK TABLES;



--
-- Dumping data for table `booking`
--

LOCK TABLES `booking` WRITE;
INSERT INTO `booking` VALUES (1,2,3,4,'2022-01-02','2022-01-05',400,'cash');
INSERT INTO `booking` VALUES (2,8,4,4,'2022-02-02','2022-02-09',1000,'credit card');
INSERT INTO `booking` VALUES (3,10,6,4,'2022-01-04','2022-01-07',500,'cash');
INSERT INTO `booking` VALUES (4,11,10,4,'2022-01-06','2022-01-07',300,'credit card');
INSERT INTO `booking` VALUES (5,12,11,13,'2022-01-11','2022-01-15',800,'cash');
INSERT INTO `booking` VALUES (6,13,12,16,'2022-01-02','2022-01-06',900,'cash');
INSERT INTO `booking` VALUES (7,14,13,4,'2022-02-02','2022-02-12',1000,'credit card');
INSERT INTO `booking` VALUES (8,15,14,17,'2022-02-10','2022-02-13',400,'credit card');
INSERT INTO `booking` VALUES (9,16,15,19,'2022-03-02','2022-03-05',300,'cash');
INSERT INTO `booking` VALUES (10,17,16,4,'2022-03-07','2022-03-09',450,'cash');
INSERT INTO `booking` VALUES (11,18,17,4,'2022-03-10','2022-03-13',600,'cash');
INSERT INTO `booking` VALUES (12,19,18,13,'2022-03-21','2022-03-25',800,'cash');
INSERT INTO `booking` VALUES (13,20,19,16,'2022-03-20','2022-03-25',900,'credit card');
INSERT INTO `booking` VALUES (14,21,20,17,'2022-04-02','2022-04-04',400,'cash');
INSERT INTO `booking` VALUES (15,2,20,19,'2022-04-04','2022-04-12',1100,'cash');
INSERT INTO `booking` VALUES (16,20,21,13,'2022-05-02','2022-01-12',1200,'credit card');
INSERT INTO `booking` VALUES (17,19,22,13,'2022-05-03','2022-05-05',400,'cash');
INSERT INTO `booking` VALUES (18,10,23,4,'2022-06-12','2022-06-15',500,'cash');
INSERT INTO `booking` VALUES (19,8,24,4,'2022-06-14','2022-06-16',400,'cash');
INSERT INTO `booking` VALUES (20,12,24,13,'2022-06-09','2022-06-12',550,'cash');
INSERT INTO `booking` VALUES (21,13,25,4,'2022-06-22','2022-06-24',400,'credit card');
INSERT INTO `booking` VALUES (22,20,25,13,'2022-07-04','2022-07-07',800,'cash');
INSERT INTO `booking` VALUES (23,21,26,4,'2022-07-09','2022-07-12',900,'cash');
INSERT INTO `booking` VALUES (24,12,27,16,'2022-07-07','2022-07-10',450,'credit card');
INSERT INTO `booking` VALUES (25,13,1,17,'2022-07-12','2022-07-14',400,'cash');
INSERT INTO `booking` VALUES (26,14,2,19,'2022-08-08','2022-08-10',420,'cash');
INSERT INTO `booking` VALUES (27,19,3,13,'2022-08-02','2022-08-04',380,'credit card');
INSERT INTO `booking` VALUES (28,10,4,4,'2022-09-02','2022-09-05',600,'credit card');
INSERT INTO `booking` VALUES (29,11,5,16,'2022-09-09','2022-09-12',700,'cash');
INSERT INTO `booking` VALUES (30,12,6,17,'2022-09-11','2022-09-16',880,'cash');
INSERT INTO `booking` VALUES (31,13,7,19,'2022-10-01','2022-10-03',400,'cash');
INSERT INTO `booking` VALUES (32,20,8,4,'2022-10-02','2022-10-07',790,'cash');
INSERT INTO `booking` VALUES (33,21,9,13,'2022-10-04','2022-10-06',400,'credit card');
INSERT INTO `booking` VALUES (34,19,10,16,'2022-11-02','2022-11-08',810,'credit card');
INSERT INTO `booking` VALUES (35,18,11,17,'2022-11-02','2022-11-05',400,'credit card');
INSERT INTO `booking` VALUES (36,2,12,4,'2022-11-04','2022-11-07',550,'credit card');
INSERT INTO `booking` VALUES (37,8,13,4,'2022-12-02','2022-12-12',980,'credit card');
INSERT INTO `booking` VALUES (38,11,14,19,'2022-12-05','2022-12-09',700,'credit card');
INSERT INTO `booking` VALUES (39,21,15,4,'2023-01-01','2023-01-04',450,'credit card');
INSERT INTO `booking` VALUES (40,17,16,19,'2023-01-02','2023-01-04',460,'cash');
UNLOCK TABLES;


--
-- Dumping data for table `meal`
--

LOCK TABLES `meal` WRITE;
INSERT INTO `meal` VALUES (1,'Instant Pot Oatmeal', 'breakfast');
INSERT INTO `meal` VALUES (2,'Avocado Toast', 'breakfast');
INSERT INTO `meal` VALUES (3,'Omlet', 'breakfast');
INSERT INTO `meal` VALUES (4,'Scrambled Eggs', 'breakfast');
INSERT INTO `meal` VALUES (5,'Bagel and Cream Cheese', 'breakfast');
INSERT INTO `meal` VALUES (6,'Bananas and Peanut Butter Pancakes', 'breakfast');
INSERT INTO `meal` VALUES (7,'Spaghetti Bolognaise', 'launch');
INSERT INTO `meal` VALUES (8,'Lasagne', 'launch');
INSERT INTO `meal` VALUES (9,'Fettuccine Alfredo', 'launch');
INSERT INTO `meal` VALUES (10,'Pasta Carbonara', 'launch');
INSERT INTO `meal` VALUES (11,'Ravioli', 'launch');
INSERT INTO `meal` VALUES (12,'Macaroni Cheese', 'launch');
INSERT INTO `meal` VALUES (13,'Pasta Cacio e Pepe', 'launch');
INSERT INTO `meal` VALUES (14,'Spaghetti alle Vongole', 'launch');
INSERT INTO `meal` VALUES (15,'Anticucho', 'launch');
INSERT INTO `meal` VALUES (16,'Asocena', 'launch');
INSERT INTO `meal` VALUES (17,'Barbacoa', 'launch');
INSERT INTO `meal` VALUES (18,'Tofu Lettuce Wraps', 'launch');
INSERT INTO `meal` VALUES (19,'Braised Chickpeas with Chard', 'launch');
INSERT INTO `meal` VALUES (20,'Tofu Stir-Fry with Peanut Sauce', 'launch');
INSERT INTO `meal` VALUES (21,'Vegetarian Meatballs', 'launch');
INSERT INTO `meal` VALUES (22,'Pizza Margherita', 'dinner');
INSERT INTO `meal` VALUES (23,'Pizza Pepperoni', 'dinner');
INSERT INTO `meal` VALUES (24,'Pizza Bufalo', 'dinner');
INSERT INTO `meal` VALUES (25,'Pizza Fritta', 'dinner');
INSERT INTO `meal` VALUES (26,'Pizza Siciliana', 'dinner');
INSERT INTO `meal` VALUES (27,'CheeseBurger', 'dinner');
INSERT INTO `meal` VALUES (28,'Hamburger', 'dinner');
UNLOCK TABLES;

--
-- Dumping data for table `order`
--

LOCK TABLES `orders` WRITE;
INSERT INTO `orders` VALUES (1,2,1,2);
INSERT INTO `orders` VALUES (2,8,2,2);
INSERT INTO `orders` VALUES (3,10,3,2);
INSERT INTO `orders` VALUES (4,11,3,21);
INSERT INTO `orders` VALUES (5,12,4,11);
INSERT INTO `orders` VALUES (6,12,3,20);
INSERT INTO `orders` VALUES (7,13,20,14);
INSERT INTO `orders` VALUES (8,8,21,14);
INSERT INTO `orders` VALUES (9,14,22,2);
INSERT INTO `orders` VALUES (10,14,23,14);
INSERT INTO `orders` VALUES (11,12,24,14);
INSERT INTO `orders` VALUES (12,21,14,18);
INSERT INTO `orders` VALUES (13,11,1,20);
INSERT INTO `orders` VALUES (14,10,2,14);
INSERT INTO `orders` VALUES (15,2,3,2);
INSERT INTO `orders` VALUES (16,2,4,11);
INSERT INTO `orders` VALUES (17,8,5,20);
UNLOCK TABLES;

--
-- Dumping data for table `clean`
--

LOCK TABLES `clean` WRITE;
INSERT INTO `clean` VALUES (1,1,1,'2022-01-02');
INSERT INTO `clean` VALUES (2,3,2,'2022-02-01');
INSERT INTO `clean` VALUES (3,5,3,'2022-03-02');
INSERT INTO `clean` VALUES (4,6,4,'2022-04-02');
INSERT INTO `clean` VALUES (5,6,11,'2022-01-02');
INSERT INTO `clean` VALUES (6,8,5,'2022-01-02');
INSERT INTO `clean` VALUES (7,9,6,'2022-01-02');
INSERT INTO `clean` VALUES (8,10,1,'2022-01-02');
INSERT INTO `clean` VALUES (9,11,6,'2022-04-12');
INSERT INTO `clean` VALUES (10,11,7,'2022-04-13');
INSERT INTO `clean` VALUES (11,6,11,'2022-03-14');
INSERT INTO `clean` VALUES (12,1,12,'2022-01-02');
INSERT INTO `clean` VALUES (13,9,13,'2022-07-02');
INSERT INTO `clean` VALUES (14,9,14,'2022-08-08');
INSERT INTO `clean` VALUES (15,9,13,'2022-09-02');
INSERT INTO `clean` VALUES (16,8,15,'2022-05-02');
INSERT INTO `clean` VALUES (17,11,16,'2022-04-02');
INSERT INTO `clean` VALUES (18,7,17,'2022-03-02');
INSERT INTO `clean` VALUES (19,7,5,'2022-02-02');
INSERT INTO `clean` VALUES (20,6,11,'2022-01-02');
UNLOCK TABLES;


/*********************************
 INSERT YOUR SOLUTIONS 
*********************************/

-- /* (1) CREATE TABLE ANSWER00 as */
CREATE TABLE ANSWER00
AS
(SELECT ID, Number 
FROM 
WHERE room.View = 'mountain view');

-- /* (a) CREATE TABLE ANSWER01 as */
CREATE TABLE ANSWER01
AS
(SELECT SSN, Name
From employee
Where employee.CivilStatus = 'single' And (employee.Name != null OR employee.Name Like 'a%'))

-- /* (b) CREATE TABLE ANSWER02 as */
-- CREATE TABLE ANSWER02
-- AS
  
  

-- /* (c) CREATE TABLE ANSWER03 as */
-- CREATE TABLE ANSWER03
-- AS
  

-- /* (d) CREATE TABLE ANSWER04 as */
-- CREATE TABLE ANSWER04
-- AS
  

-- /* (e) CREATE TABLE ANSWER05 as */
-- CREATE TABLE ANSWER05
-- AS
  

-- /* (f) CREATE TABLE ANSWER06 as */
-- CREATE TABLE ANSWER06
-- AS
  

-- /* (g) CREATE TABLE ANSWER07 as */

-- CREATE TABLE ANSWER07
-- AS 
  

-- /* (h) CREATE TABLE ANSWER08 as */
-- CREATE TABLE ANSWER08
-- AS
  

-- /* (i) CREATE TABLE ANSWER09 as */
-- CREATE TABLE ANSWER09
-- AS
  

-- /* (i) CREATE TABLE ANSWER10 as */
-- CREATE TABLE ANSWER10
-- AS

-- SELECT * FROM ANSWER00;
-- SELECT * from ANSWER01;
-- SELECT * FROM ANSWER02;
-- SELECT * FROM ANSWER03;
-- SELECT * FROM ANSWER04;
-- SELECT * FROM ANSWER05;
-- SELECT * FROM ANSWER06;
-- SELECT * FROM ANSWER07;
-- SELECT * FROM ANSWER08;
-- SELECT * FROM ANSWER09;
-- SELECT * FROM ANSWER10;
