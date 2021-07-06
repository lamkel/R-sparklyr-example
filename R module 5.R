# https://www.r-bloggers.com/2019/02/working-with-big-sas-datasets-using-r-and-sparklyr/ #
# https://www.java.com/en/download/help/win_controlpanel.html #
# https://github.com/sparklyr/sparklyr/issues/1828 #
# https://cran.r-project.org/web/packages/spark.sas7bdat/vignettes/spark_sas7bdat_examples.html #
# https://github.com/bnosac/spark.sas7bdat/issues/7 #


.libPaths("C:/Users/Lamke/LP/R library")
#install.packages(c("sparklyr", "spark.sas7bdat", "haven"))

library(sparklyr)

options(spark.install.dir = "C:/Users/Lamke/LP/spark/")
spark_install_dir()
spark_installed_versions()
#spark_available_versions() 

#spark_version <- "3.0"
#spark_install(version = spark_version, reset = TRUE)
# spark_uninstall(version = spark_version) #
Sys.setenv(JAVA_HOME="C:/Users/Lamke/LP/Java/jre1.8.0_291")
#Sys.setenv(SPARK_HOME="C:/Users/Lamke/LP/spark/spark-2.3.3-bin-hadoop2.7")
Sys.setenv(SPARK_HOME="C:/Users/Lamke/LP/spark/spark-3.0.1-bin-hadoop3.2")

conf <- spark_config() 
conf$`sparklyr.cores.local` <- 4
conf$`sparklyr.shell.driver-memory` <- "16G"
conf$spark.memory.fraction <- 0.9

# https://floatsdsds.github.io/sparklyr-install-connect-spark/ #

sc <- spark_connect(master = "local", 
                    version = "3.0.1",
                    config = list(sparklyr.verbose = TRUE))

# Install these packages for testing Spark #
# install.packages(c("nycflights13", "Lahman"))

# Load after setting up spark session #
library(readr)
library(haven)
library(spark.sas7bdat)
library(dplyr)

# Testing spark to read iris and flight data #
iris_tbl <- copy_to(sc, iris)
flights_tbl <- copy_to(sc, nycflights13::flights, "flights")
batting_tbl <- copy_to(sc, Lahman::Batting, "batting")
dplyr::src_tbls(sc)

flights_tbl %>% filter(dep_delay == 2)

# Collect final data frame delay in the global environment #
delay <- flights_tbl %>%
  group_by(tailnum) %>%
  summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
  filter(count > 20, dist < 2000, !is.na(delay)) %>%
  collect

# Try out spark.sas7bdat #
myfile <- system.file("extdata", "iris.sas7bdat", package = "spark.sas7bdat")
myfile

x <- read_sas(myfile)

x <- spark_read_sas(sc, path = myfile, table = "sas_example")
x
