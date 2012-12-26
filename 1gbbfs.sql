-- MySQL dump 10.13  Distrib 5.5.28, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: dbfs
-- ------------------------------------------------------
-- Server version	5.5.28-0ubuntu0.12.04.3

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `backend`
--

DROP TABLE IF EXISTS `backend`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `backend` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `address` varchar(255) NOT NULL,
  `port` int(11) NOT NULL,
  `storage_dir` varchar(255) NOT NULL,
  `prio_read` int(11) NOT NULL,
  `prio_write` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`(30)),
  UNIQUE KEY `address_port` (`address`(20),`port`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `backend_bgroup`
--

DROP TABLE IF EXISTS `backend_bgroup`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `backend_bgroup` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `backend` int(11) NOT NULL,
  `bgroup` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `backend_bgroup` (`backend`,`bgroup`),
  KEY `backend_bgroup_2` (`bgroup`),
  CONSTRAINT `backend_bgroup_1` FOREIGN KEY (`backend`) REFERENCES `backend` (`id`),
  CONSTRAINT `backend_bgroup_2` FOREIGN KEY (`bgroup`) REFERENCES `bgroup` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bgroup`
--

DROP TABLE IF EXISTS `bgroup`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bgroup` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `prio_read` int(11) NOT NULL,
  `prio_write` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`(30))
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `client`
--

DROP TABLE IF EXISTS `client`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `client` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`(30))
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `client_backend`
--

DROP TABLE IF EXISTS `client_backend`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `client_backend` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client` int(11) NOT NULL,
  `backend` int(11) NOT NULL,
  `prio_read` int(11) NOT NULL,
  `prio_write` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `client_backend` (`client`,`backend`),
  KEY `client_backend_2` (`backend`),
  CONSTRAINT `client_backend_1` FOREIGN KEY (`client`) REFERENCES `client` (`id`),
  CONSTRAINT `client_backend_2` FOREIGN KEY (`backend`) REFERENCES `backend` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `client_bgroup`
--

DROP TABLE IF EXISTS `client_bgroup`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `client_bgroup` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client` int(11) NOT NULL,
  `bgroup` int(11) NOT NULL,
  `prio_read` int(11) NOT NULL,
  `prio_write` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `client_bgroup` (`client`,`bgroup`),
  KEY `client_bgroup_2` (`bgroup`),
  CONSTRAINT `client_bgroup_1` FOREIGN KEY (`client`) REFERENCES `client` (`id`),
  CONSTRAINT `client_bgroup_2` FOREIGN KEY (`bgroup`) REFERENCES `bgroup` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `metadata`
--

DROP TABLE IF EXISTS `metadata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `metadata` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `fullname` varchar(255) NOT NULL,
  `mode` int(11) NOT NULL,
  `uid` int(11) NOT NULL,
  `gid` int(11) NOT NULL,
  `size` int(11) NOT NULL,
  `mtime` int(11) NOT NULL,
  `type` int(11) NOT NULL,
  `deepness` int(11) NOT NULL,
  `fh_counter` int(11) NOT NULL DEFAULT '0',
  `required_distribution` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `fullname` (`fullname`) USING HASH,
  KEY `childs` (`deepness`,`fullname`)
) ENGINE=InnoDB AUTO_INCREMENT=232480 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `reg_backend`
--

DROP TABLE IF EXISTS `reg_backend`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `reg_backend` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `metadata` int(11) DEFAULT NULL,
  `backend` int(11) NOT NULL,
  `path` varchar(255) NOT NULL,
  `max_valid_pos` int(11) NOT NULL,
  `state` int(11) NOT NULL,
  `metadata_update_tries` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `backend_path` (`backend`,`path`),
  UNIQUE KEY `metadata_backend` (`backend`,`metadata`) USING HASH,
  KEY `fk_reg_backend_metadata` (`metadata`),
  KEY `fk_reg_backend_backend` (`backend`),
  CONSTRAINT `fk_reg_backend_metadata` FOREIGN KEY (`metadata`) REFERENCES `metadata` (`id`) ON UPDATE NO ACTION,
  CONSTRAINT `fk_reg_backend_backend` FOREIGN KEY (`backend`) REFERENCES `backend` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=457935 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2012-12-26 16:32:55
