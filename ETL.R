##RFM Project ETL##
##Extracting Columns i want
library(dplyr)
library(Hmisc)
library(ggplot2)
library(scales)
Project_df <- subset(CLV_Data_join, select = c(REC_NBR, 
                                               MBRSHP_SID, 
                                               SALES, 
                                               TRIPS, 
                                               GAS_SALES, 
                                               GAS_TRIPS, 
                                               rr_1yr, 
                                               rr_2yr, 
                                               rr_3yr))

##Importing purch date data

purch_dt_data <- read.delim("C:/Users/mdunning/Documents/Data/Project_Data/SQLAExport.txt", sep = '\t',header = TRUE)
purch_dt_data2 <- read.delim("C:/Users/mdunning/Documents/Data/Project_Data/SQLAExport2.txt", sep = '\t',header = TRUE)

View(head(purch_dt_data2,50))

View(head(purch_dt_data))

?read.table
##TODO:

  ##Add purchase history (last purchase date then calculate num of days to today)
        ##Recode to quintiles

Project_df <- left_join(Project_df, good_enr_date, by = 'REC_NBR')

Project_df <- Project_df[ , -which(names(Project_df) %in% c("GAS_SALES", "GAS_TRIPS"))]


names(Project_df)[names(Project_df) == "quintile"] <- "S_Quintile" 


##joining purch_date data
##first adding MBR_NBR to projecdt_df
Project_df <- left_join(Project_df, MBR_SID_MBR_NBR, by = 'MBRSHP_SID')


##Now Joining puch_dt_data2 with included sales_QTY data where it is unformatted to date -- joining into Project_df_join
Project_df_join <- left_join(Project_df, purch_dt_data2, by = 'MBRSHP_NBR')
View(head(Project_df_join))

#Dropping duplicate column from join
Project_df_join <- Project_df_join[ , -which(names(Project_df_join) %in% c("MBRSHP_ENR_DT.x"))]
names(Project_df_join)[names(Project_df_join) == "MBRSHP_ENR_DT.y"] <- "MBRSHP_ENR_DT" 
View(head(Project_df_join))

#Reformatting Date Columns
Project_df_join$PURCH_DT <- as.Date(Project_df_join$PURCH_DT, "%m/%d/%Y")
Project_df_join$MBRSHP_ENR_DT <- as.Date(Project_df_join$MBRSHP_ENR_DT, "%m/%d/%Y")
View(head(Project_df_join))




##Extrating Individual SID Maximum Purch_dates to join back on so we have only maximum purch dates for members
Project_df2 <- Project_df_join %>% group_by(MBRSHP_SID) %>% summarise(PURCH_DT = max(PURCH_DT))

sales_qty_df <- Project_df_join %>% group_by(MBRSHP_SID) %>% summarise(SALES_QTY = sum(SALES_QTY)) 

Project_df2 <- left_join(Project_df2, sales_qty_df, by = "MBRSHP_SID")
##Re-joining on Project_df

Project_df3 <- left_join(Project_df, Project_df2, by = "MBRSHP_SID")


##Dropping Irrelavent columns
Project_df3 <- Project_df3[ , -which(names(Project_df3) %in% c("TRIPS", "SALES"))]


##Creating Trips Column for 8/20/2017 to 6/03/2018

Project_df_count_trips <- Project_df_join %>%
  group_by(MBRSHP_SID) %>%
  summarise(Trips_Purch = n())

##append trips column

Project_df4 <- left_join(Project_df3, Project_df_count_trips, by = 'MBRSHP_SID')
  

##Quintiles
##Transforming Purch_date into num of days since purchase (from 6/04/2018)
Project_df4$DSLP <- rep(as.Date("06/04/2018", "%m/%d/%Y"), times = 81735)
names(Project_df4)[names(Project_df4) == "DSLP"] <- "Todays_Date" 
##Coercing to num
Project_df4$DSLP <- as.numeric(Project_df4$DSLP)

Project_df4 <- Project_df4 %>%
  mutate(DSLP = Todays_Date - PURCH_DT)
##Drop last 456 rows due to NAs
Project_df5 <- Project_df4


##REMOVE T_Quintile section its baddddd
Project_df6 <- Project_df6[ , -which(names(Project_df6) %in% c("SALES_QTY.z"))]
##Creating DF 6
Project_df6 <- subset(Project_df5, select = c("MBRSHP_SID", "DSLP", "SALES_QTY", "Trips_Purch"))

##Replacing NAs of Columns with means (Purch date and sales qty)

Project_df7 <- Project_df6 
Project_df7<-Project_df7[-which(is.na(Project_df7$SALES_QTY)),]

##compute Quintile
#Project_df6$R_Quintile <- with(Project_df6, cut(DSLP, 
 #                                               breaks=quantile(DSLP, probs=seq(0,1, by=0.20), na.rm=TRUE),
  #                                              labels = c(1,2,3,4,5),
   #                                             include.lowest=TRUE))



#Project_df6$S_Quintile <- with(Project_df6, cut(SALES_QTY, 
 #                                           breaks=quantile(SALES_QTY, probs=seq(0,1, by=0.20), na.rm=TRUE),
  #                                          labels = c(1,2,3,4,5),
   #                                         include.lowest=TRUE))

#Project_df6$F_Quintile <- with(Project_df6, cut(Trips_Purch, 
 #                                               breaks=quantile(Trips_Purch, probs=seq(0,1, by=0.20), na.rm=TRUE),
  #                                              labels = c(1,2,3,4,5),
   #                                             include.lowest=TRUE))

##concatenate Quintiles Resulting in RFM scores
#Project_df6$RFM <- paste(Project_df6$R_Quintile, Project_df6$F_Quintile, Project_df6$S_Quintile)

# Log-transform positively-skewed variables
Project_df7$DSLP <- as.numeric(Project_df7$DSLP)
Project_df7$DSLP.log <- log(Project_df7$DSLP)
Project_df7$Trips_Purch.log <- log(Project_df7$Trips_Purch)
Project_df7$SALES_QTY.log <- Project_df7$SALES_QTY + 0.1 # can't take log(0), so add a small value to remove zeros
Project_df7$SALES_QTY.log <- log(Project_df7$SALES_QTY) 


# Z-scores
Project_df7$DSLP.z <- scale(Project_df7$DSLP.log, center=TRUE, scale=TRUE)
Project_df7$Trips_Purch.z <- scale(Project_df7$Trips_Purch.log, center=TRUE, scale=TRUE)
Project_df7$SALES_QTY <- Project_df7$SALES_QTY + 0.01
Project_df7$SALES_QTY.log <- as.numeric(Project_df7$SALES_QTY.log)
Project_df7$SALES_QTY.z <- scale(Project_df7$SALES_QTY.log, center = T, scale = T)
##Fixing Sales NaNs

sales_qty_df <- Project_df7 %>%
  subset(select = c("MBRSHP_SID", "SALES_QTY"))
sales_qty_df <- sales_qty_df %>%
  mutate(SALES_QTY_log = log(sales_qty_df$SALES_QTY))

sales_qty_df <- sales_qty_df %>%
  mutate(SALES_QTY_z = as.numeric(scale(SALES_QTY_log)))
sales_qty_df <- subset(sales_qty_df, select = c("MBRSHP_SID", "SALES_QTY_z"))


##Drop unneeded columns and coerce columns to vectors for join with sales_qty_df for sales_qty.z
Project_df7 <- Project_df7[ , -which(names(Project_df7) %in% c("SALES_QTY_ROUND"))]
Project_df7 <- Project_df7[ , -which(names(Project_df7) %in% c("SALES_QTY_z"))]
Project_df7$Trips_Purch.z <- as.vector(Project_df7$Trips_Purch.z)
Project_df7$DSLP.z <- as.vector(Project_df7$DSLP.z)
Project_df8 <- left_join(Project_df7, sales_qty_df, by = 'MBRSHP_SID')




# Original scale
scatter.1 <- ggplot(Project_df7, aes(x = Project_df7$Trips_Purch, y = Project_df7$SALES_QTY)) +
  geom_jitter(aes(colour = Project_df7$DSLP)) 
scatter.1

#Original Scale doesn't give us anything useful so we log transform to make more easily interpretable

# Log-transformed
scatter.2 <- ggplot(Project_df7, aes(x = Project_df7$Trips_Purch.log, y = Project_df7$SALES_QTY.log)) +
  geom_jitter(aes(color = Project_df7$DSLP.log))
scatter.2  

rm(scatter.1, scatter.2)

##Trying 5 ? Clusters
 j <- 5
 
models <- data.frame(k=integer(),
                     tot.withinss=numeric(),
                     betweenss=numeric(),
                     totss=numeric(),
                     rsquared=numeric())

 
##Create model 
library(plyr)
library(stats) 

Prepped <- read.csv("C:/Users/mdunning/Documents/Data/prepped_cile.csv")
Prepped2 <- left_join(Prepped, Prepped3, by = c("SALES_QTY_z" = "SALES_QTY_z", "Trips_Purch_z"="Trips_Purch.z", "DSLP_z" = "DSLP.z"))


Prepped2 <- filter(Project_df8, SALES_QTY_z > 0, Trips_Purch.z > 0, DSLP.z > 0)


Prepped3<- subset(Prepped2, select = c("MBRSHP_SID", "SALES_QTY_z", "Trips_Purch.z", "DSLP.z"))
write.csv(Prepped3, "prepped_Project_File.csv")


names(Prepped)[names(Prepped) == "DSLP.z"] <- "DSLP_z" 
names(Prepped)[names(Prepped) == "SALES_QTY.z"] <- "SALES_QTY_z" 
names(Prepped)[names(Prepped) == "Trips_Purch.z"] <- "Trips_Purch_z" 



 for (k in 1:j ) {
   
   print(k)
   
   # Run kmeans
   # nstart = number of initial configurations; the best one is used
   # $iter will return the iteration used for the final model
   output <- kmeans(Prepped, centers = k, nstart = 20)
   
   # Add cluster membership to customers dataset
   var.name <- paste("cluster", k, sep="_")
   Prepped[,(var.name)] <- output$cluster
   Prepped[,(var.name)] <- factor(Prepped[,(var.name)], levels = c(1:k))
   
   # Graph clusters
   colors <- c('red','orange','green3','deepskyblue','blue')
   cluster_graph <- ggplot(Prepped, aes(x = Prepped$Trips_Purch_z, y = Prepped$SALES_QTY_z)) +
      geom_point(aes(colour = Prepped[,(var.name)])) +
      scale_colour_manual(name = "Cluster Group", values=colors) 
   cluster_graph <- cluster_graph + xlab("Z-Score Frequency")
   cluster_graph <- cluster_graph + ylab("Z-Score Monetary Value of Customer")
   title <- paste("k-means Solution with", k, sep=" ") 
   cluster_graph <- cluster_graph + ggtitle(title)
   print(cluster_graph)
   
   
   # Cluster centers in original metrics
   
   print(title)
   cluster_centers <- ddply(Project_df6, .(Prepped[,(var.name)]), summarize,
                            monetary=round(median(SALES_QTY),2),# use median b/c this is the raw, heavily-skewed data
                            frequency=round(median(Trips_Purch),1),
                            recency=round(median(DSLP), 0))
   names(cluster_centers)[names(cluster_centers)=="Prepped[, (var.name)]"] <- "Cluster"
   print(cluster_centers)
   cat("\n")
   cat("\n")
   
   # Collect model information
   models[k,("k")] <- k
   models[k,("tot.withinss")] <- output$tot.withinss # the sum of all within sum of squares
   models[k,("betweenss")] <- output$betweenss
   models[k,("totss")] <- output$totss # betweenss + tot.withinss
   models[k,("rsquared")] <- round(output$betweenss/output$totss, 3) # percentage of variance explained by cluster membership
   assign("models", models, envir = .GlobalEnv)
   
   remove(var.name, cluster_graph, cluster_centers, title, colors)
   
   
 }

Prepped <- Prepped[ , -which(names(Prepped) %in% c("cluster_1", "cluster_2", "cluster_3", "cluster_4"))]

##Attempting to Retain MBRSHP_SID after Clustering
rm(Prepped4)
Prepped4 <- Prepped3
rownames(Prepped4) = Prepped4$MBRSHP_SID
Prepped4$MBRSHP_SID = NULL
output <- kmeans(Prepped4, centers = 5, nstart = 20)

# Add cluster membership to customers dataset
var.name <- paste("cluster", 5, sep="_")
Prepped4[,(var.name)] <- output$cluster
Prepped4[,(var.name)] <- factor(Prepped4[,(var.name)], levels = c(1:5))
Prepped4$MBRSHP_SID = rownames(Prepped4)

colors <- c('red','orange','green3','deepskyblue','blue')
cluster_graph2 <- ggplot(Prepped4, aes(x = Prepped4$Trips_Purch.z, y = Prepped4$SALES_QTY_z)) +
  geom_point(aes(colour = Prepped4[,(var.name)])) +
  scale_colour_manual(name = "Cluster Group", values=colors) 
cluster_graph2 <- cluster_graph2 + xlab("Z-Score Frequency")
cluster_graph2 <- cluster_graph2 + ylab("Z-Score Monetary Value of Customer")
title <- paste("k-means Solution with", 5, sep=" ") 
cluster_graph2 <- cluster_graph2 + ggtitle(title)
print(cluster_graph2)

##Appending Response Rates to Data Frame with Cluster nums 

ids_and_rrs <- subset(Project_df5, select = c(MBRSHP_SID:rr_3yr))
Prepped2$MBRSHP_SID <- as.numeric(Prepped2$MBRSHP_SID)
Prepped3 <- left_join(Prepped2, ids_and_rrs, by = 'MBRSHP_SID')

