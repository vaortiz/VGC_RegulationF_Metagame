using Pkg
using CSV
using DuckDB
using DataFrames
using Parquet
using Clustering
using Tidier

team_ots=DataFrame(read_parquet("/home/victor/PycharmProjects/reg_f/Transform-RegF/team_list.parquet"))
round_list=DataFrame(read_parquet("/home/victor/PycharmProjects/reg_f/Transform-RegF/round_list.parquet"))
fact_run=DataFrame(read_parquet("/home/victor/PycharmProjects/reg_f/Transform-RegF/fact_run.parquet"))


query = 

"SELECT 
    ID ,
    MAX(CASE WHEN rn = 1 THEN Pokemon ELSE NULL END) AS Pokemon_1 , 
    MAX(CASE WHEN rn = 1 THEN Objeto ELSE NULL END) AS Objeto_1 , 
    MAX(CASE WHEN rn = 1 THEN Habilidad ELSE NULL END) AS Habilidad_1 , 
    MAX(CASE WHEN rn = 1 THEN Teratipo ELSE NULL END) AS Teratipo_1 , 
    MAX(CASE WHEN rn = 1 THEN Mov1 ELSE NULL END) AS Mov1_1,
    MAX(CASE WHEN rn = 1 THEN Mov2 ELSE NULL END) AS Mov2_1,
    MAX(CASE WHEN rn = 1 THEN Mov3 ELSE NULL END) AS Mov3_1,
    MAX(CASE WHEN rn = 1 THEN Mov4 ELSE NULL END) AS Mov4_1,
    MAX(CASE WHEN rn = 2 THEN Pokemon ELSE NULL END) AS Pokemon_2,
    MAX(CASE WHEN rn = 2 THEN Objeto ELSE NULL END) AS Objeto_2,
    MAX(CASE WHEN rn = 2 THEN Habilidad ELSE NULL END) AS Habilidad_2,
    MAX(CASE WHEN rn = 2 THEN Teratipo ELSE NULL END) AS Teratipo_2,
    MAX(CASE WHEN rn = 2 THEN Mov1 ELSE NULL END) AS Mov1_2,
    MAX(CASE WHEN rn = 2 THEN Mov2 ELSE NULL END) AS Mov2_2,
    MAX(CASE WHEN rn = 2 THEN Mov3 ELSE NULL END) AS Mov3_2,
    MAX(CASE WHEN rn = 2 THEN Mov4 ELSE NULL END) AS Mov4_2,
    MAX(CASE WHEN rn = 3 THEN Pokemon ELSE NULL END) AS Pokemon_3,
    MAX(CASE WHEN rn = 3 THEN Objeto ELSE NULL END) AS Objeto_3,
    MAX(CASE WHEN rn = 3 THEN Habilidad ELSE NULL END) AS Habilidad_3,
    MAX(CASE WHEN rn = 3 THEN Teratipo ELSE NULL END) AS Teratipo_3,
    MAX(CASE WHEN rn = 3 THEN Mov1 ELSE NULL END) AS Mov1_3,
    MAX(CASE WHEN rn = 3 THEN Mov2 ELSE NULL END) AS Mov2_3,
    MAX(CASE WHEN rn = 3 THEN Mov3 ELSE NULL END) AS Mov3_3,
    MAX(CASE WHEN rn = 3 THEN Mov4 ELSE NULL END) AS Mov4_3,
    MAX(CASE WHEN rn = 4 THEN Pokemon ELSE NULL END) AS Pokemon_4,
    MAX(CASE WHEN rn = 4 THEN Objeto ELSE NULL END) AS Objeto_4,
    MAX(CASE WHEN rn = 4 THEN Habilidad ELSE NULL END) AS Habilidad_4,
    MAX(CASE WHEN rn = 4 THEN Teratipo ELSE NULL END) AS Teratipo_4,
    MAX(CASE WHEN rn = 4 THEN Mov1 ELSE NULL END) AS Mov1_4,
    MAX(CASE WHEN rn = 4 THEN Mov2 ELSE NULL END) AS Mov2_4,
    MAX(CASE WHEN rn = 4 THEN Mov3 ELSE NULL END) AS Mov3_4,
    MAX(CASE WHEN rn = 4 THEN Mov4 ELSE NULL END) AS Mov4_4,
    MAX(CASE WHEN rn = 5 THEN Pokemon ELSE NULL END) AS Pokemon_5,
    MAX(CASE WHEN rn = 5 THEN Objeto ELSE NULL END) AS Objeto_5,
    MAX(CASE WHEN rn = 5 THEN Habilidad ELSE NULL END) AS Habilidad_5,
    MAX(CASE WHEN rn = 5 THEN Teratipo ELSE NULL END) AS Teratipo_5,
    MAX(CASE WHEN rn = 5 THEN Mov1 ELSE NULL END) AS Mov1_5,
    MAX(CASE WHEN rn = 5 THEN Mov2 ELSE NULL END) AS Mov2_5,
    MAX(CASE WHEN rn = 5 THEN Mov3 ELSE NULL END) AS Mov3_5,
    MAX(CASE WHEN rn = 5 THEN Mov4 ELSE NULL END) AS Mov4_5,
    MAX(CASE WHEN rn = 6 THEN Pokemon ELSE NULL END) AS Pokemon_6,
    MAX(CASE WHEN rn = 6 THEN Objeto ELSE NULL END) AS Objeto_6,
    MAX(CASE WHEN rn = 6 THEN Habilidad ELSE NULL END) AS Habilidad_6,
    MAX(CASE WHEN rn = 6 THEN Teratipo ELSE NULL END) AS Teratipo_6,
    MAX(CASE WHEN rn = 6 THEN Mov1 ELSE NULL END) AS Mov1_6,
    MAX(CASE WHEN rn = 6 THEN Mov2 ELSE NULL END) AS Mov2_6,
    MAX(CASE WHEN rn = 6 THEN Mov3 ELSE NULL END) AS Mov3_6,
    MAX(CASE WHEN rn = 6 THEN Mov4 ELSE NULL END) AS Mov4_6
FROM 
    (SELECT 
        ID,
        Pokemon,
        Objeto,
        Habilidad,
        Teratipo,
        Mov1,
        Mov2,
        Mov3,
        Mov4,
        ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID) AS rn 
    FROM 
        team_ots
    ) AS sub 
GROUP BY 
    ID;"
    
con = DBInterface.connect(DuckDB.DB)
DuckDB.register_data_frame(con, team_ots, "team_ots")



team_list=select!(sort(filter(row -> in(row.ID, unique(fact_run[fact_run.Wins .> 5, :ID])), DataFrame(DBInterface.execute(con, query)) ), :ID)
, Not(:ID))



function jaccard(a::DataFrameRow, b::DataFrameRow)
    # Convertir los DataFrames a vectores unidimensionales
    a_vec = Vector(a)
    b_vec = Vector(b)
    
    # Calcular la intersección
    intersection = length(intersect(a_vec, b_vec))
    
    # Calcular la unión
    union2 = length(union(a_vec, b_vec))
    
    # Calcular la similitud de Jaccard
    jaccard = 1 - intersection / union2
    
    return jaccard
end

jaccard(team_list[5, :], team_list[11, :])

function jacc_mat(a::DataFrame , n::Int64)
    # Convertir los DataFrames a vectores unidimensionales
    #n = length(a)
    distances_matrix = zeros(Float64, n, n)

    for i in 1:n
        for j in i+1:n
            distancia_jaccard = jaccard(a[i, :], a[j, :])
            distances_matrix[i, j] = distancia_jaccard
            distances_matrix[j, i] = distancia_jaccard
        end
    end
    return distances_matrix
end

dist_matrix=jacc_mat(team_list, 1250)

dist_matrix
DataFrame(Tables.table(dist_matrix))

hc = hclust(dist_matrix, linkage=:complete)
Archetype = cutree(hc, k=100)

n_poks=@chain team_list begin
    @select(1,9,17,25,33,41)
    @bind_cols(_, DataFrame(Archetype=Archetype))
    @pivot_longer(Not(:Archetype), names_to=:round, values_to=:Pokemon)
    @select(1,3)
    @group_by(Archetype, Pokemon)
    @summarize(n = n())
    @ungroup()
    @arrange(Archetype, desc(n))
end

a=@chain team_list begin
    @select(1,9,17,25,33,41)
    @bind_cols(_, DataFrame(Archetype=Archetype))
    @filter (Archetype == 35)
end

b=@chain team_list begin
    @select(1,9,17,25,33,41)
    @bind_cols(_, DataFrame(Archetype=Archetype))
    @group_by (Archetype)
    @summarise (n=n())
    @ungroup
end


KOG_IDs=DataFrame(DBInterface.execute(con, "SELECT ID
FROM (
    SELECT ID, COUNT(CASE WHEN Pokemon = 'Kingambit' THEN 1 END) AS Kingambit_count,
                 COUNT(CASE WHEN Pokemon = 'Gouging Fire' THEN 1 END) AS GougingFire_count,
                 COUNT(CASE WHEN Pokemon = 'Ogerpon-Wellspring' THEN 1 END) AS OgerponWellspring_count,
                 COUNT(CASE WHEN Pokemon = 'Dragonite' THEN 1 END) AS Nite_count,
                 COUNT(CASE WHEN Pokemon = 'Entei' THEN 1 END) AS Entei_count
    FROM team_ots
    WHERE Pokemon IN ('Kingambit', 'Gouging Fire', 'Ogerpon-Wellspring' , 'Dragonite' , 'Entei')
    GROUP BY ID
) AS subquery
WHERE Kingambit_count = 1 AND GougingFire_count = 1 AND OgerponWellspring_count = 1 AND Nite_count = 0 AND Entei_count = 0;
"))


Japan_Balance_IDs=DataFrame(DBInterface.execute(con, "SELECT ID
FROM (
    SELECT ID, COUNT(CASE WHEN Pokemon = 'Urshifu-Rapid-Strike' THEN 1 END) AS Kingambit_count,
                 COUNT(CASE WHEN Pokemon = 'Rillaboom' THEN 1 END) AS GougingFire_count,
                 COUNT(CASE WHEN Pokemon = 'Incineroar' THEN 1 END) AS OgerponWellspring_count,
                 COUNT(CASE WHEN Pokemon = 'Chien-Pao' THEN 1 END) AS Pao_count,
                 COUNT(CASE WHEN Pokemon = 'Dragonite' THEN 1 END) AS Nite_count,
                 COUNT(CASE WHEN Pokemon = 'Entei' THEN 1 END) AS Entei_count

    FROM team_ots
    WHERE Pokemon IN ('Urshifu-Rapid-Strike', 'Rillaboom', 'Incineroar', 'Chien-Pao' , 'Dragonite' , 'Entei')
    GROUP BY ID
) AS subquery
WHERE Kingambit_count = 1 AND GougingFire_count = 1 AND OgerponWellspring_count = 1 AND Pao_count = 1 AND Nite_count = 0 AND Entei_count = 0;
"))

FWG_TR_IDs=DataFrame(DBInterface.execute(con, """
SELECT ID
FROM (
    SELECT ID,
           SUM(CASE WHEN Pokemon = 'Urshifu-Rapid-Strike' THEN 1 ELSE 0 END) AS Urshifu_count,
           SUM(CASE WHEN Pokemon = 'Rillaboom' THEN 1 ELSE 0 END) AS Rillaboom_count,
           SUM(CASE WHEN Pokemon = 'Incineroar' THEN 1 ELSE 0 END) AS Incineroar_count
    FROM team_ots
    WHERE ID IN (
        SELECT DISTINCT ID
        FROM team_ots
        WHERE Mov1 = 'Trick Room' OR Mov2 = 'Trick Room' OR Mov3 = 'Trick Room' OR Mov4 = 'Trick Room'
    )
    GROUP BY ID
) AS subquery
WHERE Urshifu_count = 1 AND Rillaboom_count = 1 AND Incineroar_count = 1
"""))

TW_Offense_IDs=DataFrame(DBInterface.execute(con, "SELECT ID
FROM (
    SELECT ID,
           COUNT(CASE WHEN Pokemon = 'Tornadus' THEN 1 END) AS Kingambit_count,
           COUNT(CASE WHEN Pokemon = 'Flutter Mane' THEN 1 END) AS GougingFire_count,
           COUNT(CASE WHEN Pokemon = 'Urshifu' THEN 1 END) AS OgerponWellspring_count
    FROM team_ots
    WHERE Pokemon IN ('Flutter Mane', 'Tornadus', 'Urshifu')
    GROUP BY ID
) AS subquery
WHERE Kingambit_count = 1 
  AND GougingFire_count = 1 
  AND OgerponWellspring_count = 1 
  AND ID NOT IN (
    SELECT ID
    FROM team_ots
    WHERE Pokemon IN ('Dondozo', 'Iron Crown', 'Ursaluna-Bloodmoon')
);
"))

DozoLu_IDs=DataFrame(DBInterface.execute(con, "SELECT ID
FROM (
    SELECT ID, COUNT(CASE WHEN Pokemon = 'Dondozo' THEN 1 END) AS Kingambit_count,
                 COUNT(CASE WHEN Pokemon = 'Ting-Lu' THEN 1 END) AS GougingFire_count
    FROM team_ots
    WHERE Pokemon IN ('Dondozo', 'Ting-Lu')
    GROUP BY ID
) AS subquery
WHERE Kingambit_count = 1 AND GougingFire_count = 1;
"))


DozoTatsu_IDs=DataFrame(DBInterface.execute(con, """
SELECT ID
FROM (
    SELECT ID, 
           COUNT(CASE WHEN Pokemon = 'Dondozo' THEN 1 END) AS Dondozo_count,
           COUNT(CASE WHEN Pokemon IN ('Tatsugiri', 'Tatsugiri-Droopy', 'Tatsugiri-Stretchy') THEN 1 END) AS Tatsugiri_count
    FROM team_ots
    WHERE Pokemon IN ('Dondozo', 'Tatsugiri', 'Tatsugiri-Droopy', 'Tatsugiri-Stretchy')
    GROUP BY ID
) AS subquery
WHERE Dondozo_count = 1 AND Tatsugiri_count = 1
"""))

Gholdengo_HO_IDs = DataFrame(DBInterface.execute(con, """
SELECT DISTINCT ID
FROM (
    SELECT ID, 
           COUNT(CASE WHEN Pokemon = 'Gholdengo' THEN 1 END) AS Gholdengo_count,
           COUNT(CASE WHEN Pokemon = 'Tornadus' THEN 1 END) AS Tornadus_count
    FROM team_ots
    WHERE Pokemon IN ('Gholdengo', 'Tornadus')
    AND NOT (Pokemon = 'Tornadus' AND (Mov1 = 'Rain Dance' OR Mov2 = 'Rain Dance' OR Mov3 = 'Rain Dance' OR Mov4 = 'Rain Dance'))
    AND ID NOT IN (
        SELECT ID
        FROM team_ots
        WHERE Pokemon = 'Flutter Mane'
    )
    GROUP BY ID
) AS subquery
WHERE Gholdengo_count = 1 AND Tornadus_count = 1;
"""))

Spanish_Balance_IDs = DataFrame(DBInterface.execute(con, """
SELECT ID
FROM (
    SELECT ID,
           SUM(CASE WHEN Pokemon = 'Incineroar' THEN 1 ELSE 0 END) AS Incineroar_count,
           SUM(CASE WHEN Pokemon = 'Amoonguss' THEN 1 ELSE 0 END) AS Amoonguss_count,
           SUM(CASE WHEN Pokemon = 'Raging Bolt' AND Objeto = 'Assault Vest' THEN 1 ELSE 0 END) AS Raging_Bolt_count,
           SUM(CASE WHEN Pokemon = 'Roaring Moon' THEN 1 ELSE 0 END) AS Roaring_count
    FROM team_ots
    GROUP BY ID
) AS subquery
WHERE Incineroar_count = 1 AND Amoonguss_count = 1 AND Raging_Bolt_count = 1 AND Roaring_count = 0;
"""))



Rain_Offense_IDs = DataFrame(DBInterface.execute(con, """
SELECT DISTINCT ID
FROM (
    SELECT ID, 
           COUNT(CASE WHEN Pokemon = 'Pelipper' THEN 1 END) AS Dondozo_count,
           COUNT(CASE WHEN Pokemon = 'Archaludon' THEN 1 END) AS Tatsugiri_count,
           COUNT(CASE WHEN Pokemon = 'Urshifu-Rapid-Strike' THEN 1 END) AS Urshifu_count
    FROM team_ots
    WHERE Pokemon IN ('Pelipper', 'Archaludon', 'Urshifu-Rapid-Strike')
    GROUP BY ID
) AS subquery
WHERE Dondozo_count = 1 AND Tatsugiri_count = 1 AND Urshifu_count =1
"""))


Pao_Prio_IDs = DataFrame(DBInterface.execute(con, """
SELECT ID
FROM (
    SELECT ID,
           SUM(CASE WHEN Pokemon = 'Chien-Pao' THEN 1 ELSE 0 END) AS Urshifu_count
    FROM team_ots
    WHERE ID IN (
        SELECT DISTINCT ID
        FROM team_ots
        WHERE (Mov1 = 'Extreme Speed' OR Mov2 = 'Extreme Speed' OR Mov3 = 'Extreme Speed' OR Mov4 = 'Extreme Speed')
        AND ID NOT IN (
            SELECT ID
            FROM team_ots
            WHERE Pokemon IN ('Tatsugiri', 'Dondozo')
        )
    )
    GROUP BY ID
) AS subquery
WHERE Urshifu_count = 1;
"""))

Psyspam_TW_IDs = DataFrame(DBInterface.execute(con, """
SELECT DISTINCT ID
FROM (
    SELECT ID, 
           COUNT(CASE WHEN Pokemon = 'Indeedee-F' THEN 1 END) AS Dondozo_count,
           COUNT(CASE WHEN Pokemon = 'Iron Crown' THEN 1 END) AS Tatsugiri_count,
           COUNT(CASE WHEN Pokemon = 'Tornadus' THEN 1 END) AS Urshifu_count
    FROM team_ots
    WHERE Pokemon IN ('Indeedee-F', 'Tornadus', 'Iron Crown')
    GROUP BY ID
) AS subquery
WHERE Dondozo_count = 1 AND Tatsugiri_count = 1 AND Urshifu_count =1
"""))

TR_Balance_IDs = DataFrame(DBInterface.execute(con, """
SELECT DISTINCT ID
FROM (
    SELECT ID,
           SUM(CASE WHEN Pokemon IN ('Amoonguss' , 'Ogerpon-Wellspring') THEN 1 ELSE 0 END) AS Amoonguss_count,
           SUM(CASE WHEN Pokemon = 'Incineroar' THEN 1 ELSE 0 END) AS Incineroar_count,
           SUM(CASE WHEN Pokemon IN ('Cresselia', 'Porygon2', 'Bronzong' , 'Farigiraf') THEN 1 ELSE 0 END) AS First_group_count,
           SUM(CASE WHEN Pokemon IN ('Ursaluna', 'Ursaluna-Bloodmoon', 'Iron Hands', 'Gholdengo') THEN 1 ELSE 0 END) AS Second_group_count
    FROM team_ots
    GROUP BY ID
) AS subquery
WHERE Amoonguss_count = 1 AND Incineroar_count = 1 
AND (First_group_count >= 1 AND Second_group_count >= 1);"""))


HardTR_IDs = DataFrame(DBInterface.execute(con, """
SELECT ID
FROM (
    SELECT ID
    FROM team_ots
    WHERE Mov1 = 'Expanding Force' OR Mov2 = 'Expanding Force' OR Mov3 = 'Expanding Force' OR Mov4 = 'Expanding Force'
    GROUP BY ID
) AS subquery
WHERE ID IN (
    SELECT ID
    FROM team_ots
    WHERE Pokemon IN ('Indeedee-F', 'Torkoal')
    GROUP BY ID
    HAVING COUNT(DISTINCT Pokemon) = 2
);"""))

TurboMoona_IDs = DataFrame(DBInterface.execute(con, """
SELECT ID
FROM (
    SELECT ID,
           SUM(CASE WHEN Pokemon = 'Tornadus' THEN 1 ELSE 0 END) AS Tornadus_count,
           SUM(CASE WHEN Pokemon = 'Ursaluna-Bloodmoon' THEN 1 ELSE 0 END) AS Ursaluna_Bloodmoon_count,
           SUM(CASE WHEN Pokemon = 'Farigiraf' THEN 1 ELSE 0 END) AS Farigiraf_count
    FROM team_ots
    GROUP BY ID
) AS subquery
WHERE Tornadus_count >= 1 AND Ursaluna_Bloodmoon_count >= 1 AND Farigiraf_count >= 1;

"""))

SetUp_DD_IDs = DataFrame(DBInterface.execute(con, """
SELECT ID
FROM (
    SELECT ID
    FROM team_ots
    WHERE (Mov1 = 'Dragon Dance' AND Pokemon = 'Roaring Moon') OR 
          (Mov2 = 'Dragon Dance' AND Pokemon = 'Roaring Moon') OR 
          (Mov3 = 'Dragon Dance' AND Pokemon = 'Roaring Moon') OR 
          (Mov4 = 'Dragon Dance' AND Pokemon = 'Roaring Moon')
    GROUP BY ID
) AS subquery
WHERE ID IN (
    SELECT ID
    FROM team_ots
    WHERE Pokemon IN ('Incineroar', 'Ogerpon-Wellspring')
    GROUP BY ID
    HAVING COUNT(DISTINCT Pokemon) = 2
);

"""))

Hail_Offense_IDs = DataFrame(DBInterface.execute(con, """
SELECT ID
FROM (
    SELECT ID,
           MAX(CASE WHEN Pokemon = 'Ninetales-Alola' THEN 1 ELSE 0 END) AS Ninetales_Alola_present,
           MAX(CASE WHEN Pokemon = 'Articuno' AND (Mov1 = 'Blizzard' OR Mov2 = 'Blizzard' OR Mov3 = 'Blizzard' OR Mov4 = 'Blizzard') THEN 1 ELSE 0 END) AS Articuno_present,
           MAX(CASE WHEN Pokemon = 'Iron Bundle' AND (Mov1 = 'Blizzard' OR Mov2 = 'Blizzard' OR Mov3 = 'Blizzard' OR Mov4 = 'Blizzard') THEN 1 ELSE 0 END) AS Iron_Bundle_present,
           MAX(CASE WHEN Pokemon IN ('Baxcalibur') THEN 1 ELSE 0 END) AS Other_Pokemon_present,
           MAX(CASE WHEN Pokemon = 'Dondozo' THEN 1 ELSE 0 END) AS Dondozo_present
    FROM team_ots
    GROUP BY ID
) AS subquery
WHERE Ninetales_Alola_present = 1 
    AND ((Articuno_present = 1 AND Iron_Bundle_present = 0) OR (Articuno_present = 0 AND Iron_Bundle_present = 1) OR Other_Pokemon_present = 1)
    AND Dondozo_present = 0;

"""))


df_names = ["Hail_Offense_IDs", "KOG_IDs", "Japan_Balance_IDs", "FWG_TR_IDs", 
            "TW_Offense_IDs", "DozoLu_IDs", "DozoTatsu_IDs", "Gholdengo_HO_IDs", 
            "Spanish_Balance_IDs", "Rain_Offense_IDs", "Pao_Prio_IDs", 
            "Psyspam_TW_IDs", "TR_Balance_IDs", "HardTR_IDs", "TurboMoona_IDs", "SetUp_DD_IDs"]

merged_df = DataFrame(ID = String[], DF_Name = String[])
merged_df


# Fusiona todos los DataFrames en uno solo
for df_name in df_names
    df = eval(Symbol(df_name))
    df_temp = DataFrame(ID = df.ID, DF_Name = df_name)
    append!(merged_df, df_temp)
end



fact_run_fix=rename!(sort(leftjoin(fact_run, merged_df, on = :ID), :ID), :DF_Name => :Archetype)

CSV.write("fact_run_fix.csv" , fact_run_fix)

