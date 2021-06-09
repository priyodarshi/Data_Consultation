install.packages("tidyverse")
install.packages("dplyr")
install.packages("sqldf")


library(tidyverse) 
library(dplyr)
library(sqldf)


 vax <- read.csv("C:/Users/priyo/Downloads/dataxp/country_vaccinations_csv.csv")
head(vax)

vax$daily_vac <- coalesce(vax$daily_vaccinations_raw, vax$daily_vaccinations, vax$total_vaccinations)
glimpse(vax)
write.csv(vax, file= "dataxp_vax", row.names = FALSE)
setwd("C:/Users/priyo/Downloads/dataxp")


sql_vax <- sqldf('select country
                    ,iso_code
                    ,date
                    ,daily_vac
                    ,total_vaccinations
                    ,vaccines
            FROM vax')
sql_vax
glimpse(sql_vax)



# sql_vax_clean <- sqldf('select
#                         distinct country
#                             ,MAX(total_vaccinations) as max
#                             ,SUM(daily_vac) as daily_sum
#           ,ABS((MAX(total_vaccinations)-SUM(daily_vac))/MAX(total_vaccinations)) as perc_diff
#                         FROM sql_vax
#                         GROUP BY 1
#                         ORDER BY 1')


sql_vax_clean_new <- read.csv("C:/Users/priyo/Downloads/dataxp/percentage_difference.csv")
sql_vax_clean_new


vax_clean2 <- sqldf('select *
                    ,SUM(daily_vac) OVER (PARTITION BY country ORDER BY country,date) as cumulative_vac
                    FROM sql_vax')
glimpse(vax_clean2)
write.csv(vax_clean2, file= "dataxp_vax_clean2", row.names = FALSE)


population <- read.csv("C:/Users/priyo/Downloads/dataxp/population_by_country_2020.csv", header=FALSE)

population


names(population)[1] <- 'country'
names(population)[2] <- 'population'
names(population)[5] <- 'density'
names(population)[9] <- 'medianage'
head(population)

pop <- sqldf('select
                country
                ,population
                ,density
                ,medianage
            FROM population
            ')
head(pop)
pop


vax_clean3 <- sqldf('select *
                    FROM vax_clean2
                    LEFT JOIN pop USING (country)
                    WHERE cumulative_vac > 0
                    ')
(vax_clean3)

vax_clean3$medianage[is.na(vax_clean3$medianage)] <- 0
vax_clean3$medianage
vax_clean3



write.csv(vax_clean3, file= "dataxp_vax_clean3", row.names = FALSE, na=" ")


vax_clean3 <- sqldf('select *
                    ,DENSE_RANK() OVER (PARTITION BY country ORDER BY country,date) as day
                    ,cumulative_vac/population as vac_pop_perc
                    FROM vax_clean3')

head(vax_clean3)
    download.csv(vax_clean3)


     vax_clean3_last <- read.csv("C:/Users/priyo/Downloads/dataxp/vax_clean3_last.csv")
vax_clean3_last    


winner2 <- sqldf('select *
                , 9000000 as win_pop
                FROM vax_clean3_last')
winner2
write.csv(winner, file= "winner2", row.names = FALSE)
winner3 <- sqldf('select
                    distinct country
                    ,(population - win_pop)/win_pop as pop_score
                FROM winner2')
head(winner3)
download.csv(winner)
download.csv(winner1)

write.csv(winner1, file= "winner3", row.names = FALSE)


