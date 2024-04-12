#Regularizar todo según los datos oficiales para hacerles un join posteriormente

#####################################
######## FASE DE EXPLORACIÓN ########
#####################################

library(readr)
library(tidyverse)
library(data.table)
library(arrow)
############# Fact-Run: tabla de hechos. Una fila es un jugador en un torneo

fact_run_ofi <- read_csv("~/PycharmProjects/reg_f/Extract(ScrapingAPI)/fact_run_ofi.csv")
fact_run_noofi <- read_csv("~/PycharmProjects/reg_f/Extract(ScrapingAPI)/fact_run_noofi.csv")

# ID: tienen que ser únicos

fact_run_ofi$ID %>% unique() %>% length() == nrow(fact_run_ofi) #TRUE
fact_run_noofi$ID %>% unique() %>% length() == nrow(fact_run_noofi) #TRUE

# Torneo: checkear las categorias

fact_run_ofi %>%  group_by(Torneo) %>%  summarise(n_players= n())
fact_run_noofi %>%  group_by(Torneo) %>%  summarise(n_players= n())

# Tipo_Torneo: solo hay 2
fact_run_ofi %>%  group_by(Tipo_Torneo) %>%  summarise(n= n())
fact_run_noofi %>%  group_by(Tipo_Torneo) %>%  summarise(n= n())

# Wins, Losses: número asumible, suma consistente
fact_run_ofi %>% select(Wins, Losses) %>% mutate(partidas = Wins + Losses) %>% arrange (-partidas)
fact_run_noofi %>% select(Wins, Losses) %>% mutate(partidas = Wins + Losses) %>% arrange (-partidas)

############# Rounds: una fila es una ronda asociada a un jugador en un torneo, contra un rival y obteniendo un resultado

round_list_ofi <- read_csv("~/PycharmProjects/reg_f/Extract(ScrapingAPI)/round_list_ofi.csv")
round_list_noofi <- read_csv("~/PycharmProjects/reg_f/Extract(ScrapingAPI)/round_list_noofi.csv")

# ID: tienen que coincidir con los de la fact table

round_list_ofi$ID %>% unique() %>% length() == nrow(fact_run_ofi) #TRUE
round_list_noofi$ID %>% unique() %>% length() == nrow(fact_run_noofi) #TRUE
round_list_noofi %>% filter(ID == "NOf-1220") %>% nrow == (fact_run_noofi %>% filter(ID == "NOf-1220") %>% mutate(n=Wins+Losses) %>% .$n)
sum(fact_run_noofi$Wins) + sum(fact_run_noofi$Losses)
# ROnda: Verificar si las categorias son iguales

round_list_ofi%>% group_by(Ronda) %>% summarise(n = n())
round_list_noofi%>% group_by(Ronda) %>% summarise(n = n()) # Las categorias de rondas eliminatorias son distintas

round_list_noofi = round_list_noofi %>%
  mutate(Ronda = case_when(
    grepl("^T32-", Ronda) ~ "Top 32",
    grepl("^T16-", Ronda) ~ "Top 16",
    grepl("^T8-", Ronda) ~ "Top 8",
    grepl("^T4-", Ronda) ~ "Top 4",
    grepl("^T2-", Ronda) ~ "Finals",
    TRUE ~ Ronda
  ))

round_list_noofi%>% group_by(Ronda) %>% summarise(n = n()) #ahora si coinciden

# Resultado solo se puede ganar perder o asumir un DROP

round_list_ofi %>% .$Resultado %>% table
round_list_ofi <- round_list_ofi %>%
  mutate(Resultado = ifelse(Resultado == "Ongoing", "DROP", Resultado))
round_list_noofi %>% .$Resultado %>% table

############# Team List: Cada fila es un Pokemon, un jugador en un torneo necesita 6 para participar, mínimo 4

team_list_ofi <- read_csv("~/PycharmProjects/reg_f/Extract(ScrapingAPI)/team_list_ofi.csv")
team_list_noofi <- read_csv("~/PycharmProjects/reg_f/Extract(ScrapingAPI)/team_list_noofi.csv")


# Pokemon (muchos problemas jaja)
team_list_ofi%>% group_by(Pokemon) %>% summarise(n = n()) %>% arrange(n) %>% View
team_list_ofi %>%
  filter(grepl("[^a-zA-Z]", Pokemon)) %>%  
  group_by(Pokemon) %>% 
  summarise(n = n()) %>% 
  arrange(n) %>% View

team_list_noofi%>% group_by(Pokemon) %>% summarise(n = n()) %>% arrange(n) %>% View
team_list_noofi %>%
  filter(grepl("[^a-zA-Z]", Pokemon)) %>%  
  group_by(Pokemon) %>% 
  summarise(n = n()) %>% 
  arrange(n) %>% View

team_list_noofi %>%
  mutate(
    Pokemon = case_when(
      grepl("Rapid Strike Urshifu", Pokemon) ~ "Urshifu-Rapid-Strike",
      grepl("Bloodmoon Ursaluna", Pokemon) ~ "Ursaluna-Bloodmoon",
      grepl("Indeedee ♀", Pokemon) ~ "Indeedee-F",
      grepl("Paldean Tauros (Aqua|Blaze) Breed", Pokemon) ~ gsub("Paldean Tauros (Aqua|Blaze) Breed", "Tauros-Paldea-\\1", Pokemon),
      grepl("^(Wellspring|Hearthflame|Cornerstone) Mask Ogerpon", Pokemon) ~ gsub("^(Wellspring|Hearthflame|Cornerstone) Mask Ogerpon", "Ogerpon-\\1", Pokemon),
      grepl("(Landorus|Thundurus|Tornadus|Enamorus) Therian", Pokemon) ~ gsub("(Landorus|Thundurus|Tornadus|Enamorus) Therian", "\\1-Therian", Pokemon),
      grepl("Tatsugiri (Droopy|Stretchy) Form", Pokemon) ~ gsub("Tatsugiri (Droopy|Stretchy) Form", "Tatsugiri-\\1", Pokemon),
      grepl("(Frost|Wash|Heat|Mow|Fan) Rotom", Pokemon) ~ gsub("(Frost|Wash|Heat|Mow|Fan) Rotom", "Rotom-\\1", Pokemon),
      grepl("(Alolan|Hisuian|Galarian) [A-Z][a-z]+", Pokemon) ~ {
        pokemon <- gsub("(Alolan|Hisuian|Galarian) ([A-Z][a-z]+)", "\\2-\\1", Pokemon)
        pokemon <- gsub("Galarian", "Galar", pokemon)
        pokemon <- gsub("Hisuian", "Hisui", pokemon)
        pokemon <- gsub("Alolan", "Alola", pokemon)
        pokemon
      },
      grepl("Ogerpon", Pokemon) & grepl("Hearthflame", Objeto) ~ "Ogerpon-Hearthflame",
      grepl("Ogerpon", Pokemon) & grepl("Wellspring", Objeto) ~ "Ogerpon-Wellspring",
      grepl("Ogerpon", Pokemon) & grepl("Cornerstone", Objeto) ~ "Ogerpon-Cornerstone",
      TRUE ~ Pokemon
    )
  ) -> team_list_noofi

setdiff(team_list_noofi$Pokemon , team_list_ofi$Pokemon) #Todo solucionado

# Objeto

setdiff(team_list_noofi$Objeto , team_list_ofi$Objeto) 

team_list_noofi %>%
  mutate(
    Objeto = case_when(
      grepl("Booster energy", Objeto) ~ "Booster Energy",
      grepl("choice scarf", Objeto) ~ "Choice Scarf",
      grepl("WellSpring Mask", Objeto) ~ "Wellspring Mask"
      ,
      TRUE ~ Objeto
    )
  ) ->team_list_noofi # Todo ok

# Habilidad

setdiff(team_list_ofi$Habilidad ,team_list_noofi$Habilidad) 
setdiff(team_list_noofi$Habilidad ,team_list_ofi$Habilidad) 

team_list_noofi %>%
  mutate(
    Habilidad = case_when(
      grepl("Mind's Eye", Habilidad) ~ "Mind’s Eye" ,
      grepl("Snow warning", Habilidad) ~ "Snow Warning",
      grepl("Armor tail", Habilidad) ~ "Armor Tail",
      grepl("regenerator", Habilidad) ~ "Regenerator"
      ,
      TRUE ~ Habilidad
    )
  ) -> team_list_noofi

# Teratipo

setdiff(team_list_ofi$Teratipo ,team_list_noofi$Teratipo) 
setdiff(team_list_noofi$Teratipo ,team_list_ofi$Teratipo) 

team_list_noofi %>%
  mutate(
    Teratipo = case_when(
      grepl("grass", Teratipo) ~ "Grass" ,
      grepl("psychic", Teratipo) ~ "Psychic",
      grepl("fire", Teratipo) ~ "Fire"
      ,
      TRUE ~ Teratipo
    )
  ) -> team_list_noofi #fixed no ofi , falta ofi que tiene unos strings raros en Stellar

# Mov1
setdiff(team_list_ofi$Mov1 ,team_list_noofi$Mov1) 
setdiff(team_list_noofi$Mov1 ,team_list_ofi$Mov1) 


team_list_ofi %>%
  slice(which(rowSums(is.na(select(., 6:9))) > 0)) 

team_list_ofi %>%
  slice(which(rowSums(is.na(select(., 6:9))) > 0)) %>%
  filter(grepl("^Stellar\n-", Teratipo))

team_list_ofi %>%
  mutate(
    Mov4 = ifelse(grepl("Stellar\n-", Teratipo),gsub("^Stellar\n- ", "", Teratipo), Mov4),
    Teratipo = ifelse(grepl("Stellar\n-", Teratipo), "Stellar", Teratipo)
  ) ->team_list_ofi #fixea Tera de ofi y aparte el Mov4

# Movimientos

team_list_ofi %>% select(6:9) %>% pivot_longer(everything() , names_to = "nn", values_to = "Movs") %>% select(Movs)

setdiff(team_list_noofi %>% select(6:9) %>% pivot_longer(everything() , names_to = "nn", values_to = "Movs") %>% select(Movs)
, team_list_ofi %>% select(6:9) %>% pivot_longer(everything() , names_to = "nn", values_to = "Movs") %>% select(Movs)
) 

team_list_noofi %>%
  mutate_at(vars(c("Mov1", "Mov2", "Mov3", "Mov4")), .funs = function(columna) {
    gsub("follow me", "Follow Me", columna) %>%
      gsub("Icicle crash", "Icicle Crash", .) %>%
      gsub("twin beam", "Twin Beam", .) %>%
      gsub("Sword Dance", "Swords Dance", .) %>%
      gsub("Play rough", "Play Rough", .) %>%
      gsub("Ice spinner", "Ice Spinner", .) %>%
      gsub("shelter", "Shelter", .) %>%
      gsub("U-Turn", "U-turn", .) %>%
      gsub("Horn-Leech", "Horn Leech", .)
  }) ->team_list_noofi #fixed todo

fact_run = rbind(fact_run_ofi,fact_run_noofi)
team_list = rbind(team_list_ofi,team_list_noofi)
round_list = rbind(round_list_ofi,round_list_noofi)

arrow::write_parquet(fact_run , "fact_run.parquet")
arrow::write_parquet(team_list , "team_list.parquet")
arrow::write_parquet(round_list , "round_list.parquet")

