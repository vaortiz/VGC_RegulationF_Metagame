using Pkg
Pkg.add("Tidier")

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

@chain team_list begin
    @select(1,9,17,25,33,41)
    @bind_cols(_, DataFrame(Archetype=Archetype))
    @filter (Archetype == 80)
end


kmeds = kmedoids(dist_matrix, 100)
Archetype = kmeds.assignments

dbs = dbscan(dist_matrix, 0.29, metric = nothing ,  min_neighbors = 3)
Archetype = dbs.assignments

archetype_fix= @chain team_list begin
    @bind_cols(_, DataFrame(Archetype=Archetype))
end
CSV.write("archetype.csv", archetype_fix)