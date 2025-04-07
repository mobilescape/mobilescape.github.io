library(arrow)
library(tidyverse)
library(hms)
library(igraph)
library(ggnetwork)
library(intergraph)
library(duckdb)

setwd('D:/')

con <- dbConnect(duckdb())

duckdb_register(con, "night_bike_view", "SELECT * FROM read_parquet('Seoul bike 2024/**/*.parquet') WHERE extract('hour' FROM 대여일시) < 7 OR extract('hour' FROM 대여일시) > 22;")

tmp <- dbGetQuery(con, "SELECT * FROM night_bike_view LIMIT 1000;")


tmp <- dbGetQuery(con, 
"SELECT COUNT(*) count, EXTRACT(DOW FROM 대여일시 - INTERVAL '7 hours') AS dow, EXTRACT(HOUR FROM 대여일시) AS hour
FROM read_parquet('Seoul bike 2024/**/*.parquet')
WHERE extract('hour' FROM 대여일시) < 7 OR extract('hour' FROM 대여일시) > 22
GROUP BY EXTRACT(DOW FROM 대여일시 - INTERVAL '7 hours'), EXTRACT(HOUR FROM 대여일시);")

# sunday (0) to Saturday (6)


tmp |>
  mutate(hour = ordered(hour, levels = c(23, 0, 1, 2, 3, 4, 5, 6))) |>
ggplot(aes(x = factor(hour), y = count, group = 1)) +
  geom_line() + facet_wrap(~dow)
  geom_col(aes(x = hour, y = count, fill = dow), position = 'dodge') +
  scale_x_continuous(breaks = seq(0, 23, 1)) +
  scale_y_continuous(breaks = seq(0, 10000, 1000)) +
  labs(x = 'Hour', y = 'Count', title = 'Night bike usage by hour and day of week') +
  theme_minimal() +
  theme(legend.position = "bottom") +
  scale_fill_discrete(name = "Day of Week", labels = c("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"))



tbl(con, "read_parquet('Seoul bike 2024/**/*.parquet', hive_partitioning = true)") |>
  filter(hour(대여일시) < 7 | hour(대여일시) > 22) |>
  mutate(
    wday = wday(대여일시- hour(7), label = TRUE))
|> group_by(wday, hour(대여일시)) |>
  summarise(
    count = n()
  ) |> collect()



duckdb_register(con, 'df', 'Seoul bike 2024')

SELECT * FROM read_parquet('Seoul bike 2024/**/*.parquet');



# 원격 접속
# setwd('Z:/HDD1')

list.files()

df <- open_dataset('Seoul bike 2024')

tmp <- head(df) |> collect()

tmp |>
  filter(hour(대여일시) < 7 | hour(대여일시) > 22)

head(df)


# 데이터 꺼내기 (새벽시간대)
# df <- df |>
#   filter(hour(대여일시) < 7 | hour(대여일시) > 22) |>
#   collect()
# 
# head(df)
# 
# df <- arrow_table(df)

# 데이터 저장하기
# write_parquet(df, 'Seoul_bike_2024_night.parquet')

# 저장한 데이터 불러오기
df <- read_parquet('Seoul_bike_2024_night.parquet', as_data_frame = FALSE)

df |> glimpse()

tmp <- head(df) |> collect()

tmp

tmp |> mutate(
  wday = wday(대여일시-hms(7, 0, 0), label = TRUE)) |>
  View()



edges <- df |>
  select(from=`대여 대여소번호`, to = 반납대여소번호) |>
  filter(!is.na(from) & !is.na(to)) |>
  collect()

edges <- edges |>
  group_by(from, to) |>
  summarise(weight = n())

members <- data.frame(name = unique(c(edges$from, edges$to))) # members에 위도 경도 붙이기
