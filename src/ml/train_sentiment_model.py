from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

INPUT_PATH = "hdfs://namenode:9000/data/lahjalens/ml/input/comments_ml.csv"
MODEL_PATH = "hdfs://namenode:9000/data/lahjalens/ml/models/sentiment_lr"

spark = (
    SparkSession.builder
    .appName("LahjaLens-Sentiment-Classification")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("\n=== 1. CHARGEMENT DU DATASET ===")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT_PATH)
    .select("comment_text", "target_sentiment")
    .filter(
        F.col("comment_text").isNotNull()
        & F.col("target_sentiment").isin(-1, 0, 1)
    )
    .withColumn(
        "label",
        F.when(F.col("target_sentiment") == -1, 0.0)
         .when(F.col("target_sentiment") == 0, 1.0)
         .otherwise(2.0)
    )
    .select("comment_text", "label")
    .cache()
)

total = df.count()
print(f"Nombre de commentaires : {total}")

print("\n=== 2. DISTRIBUTION DES CLASSES ===")
class_counts = {
    int(row["label"]): row["count"]
    for row in df.groupBy("label").count().collect()
}
for label, name in [(0, "Négatif"), (1, "Neutre"), (2, "Positif")]:
    print(f"{name}: {class_counts.get(label, 0)}")

if len(class_counts) != 3:
    raise ValueError("Les trois classes sont nécessaires pour entraîner le modèle.")

print("\n=== 3. DECOUPAGE STRATIFIE 80/20 ===")

train_parts = []
test_parts = []

for label in [0, 1, 2]:
    class_df = df.filter(F.col("label") == float(label))
    train_part, test_part = class_df.randomSplit([0.8, 0.2], seed=42)
    train_parts.append(train_part)
    test_parts.append(test_part)

train = train_parts[0]
test = test_parts[0]

for part in train_parts[1:]:
    train = train.unionByName(part)

for part in test_parts[1:]:
    test = test.unionByName(part)

train = train.cache()
test = test.cache()

print("Train :", train.count())
print("Test :", test.count())

print("\n=== 4. PONDERATION DES CLASSES ===")

train_counts = {
    int(row["label"]): row["count"]
    for row in train.groupBy("label").count().collect()
}

train_total = sum(train_counts.values())
weights = {
    label: train_total / (3.0 * count)
    for label, count in train_counts.items()
}

weight_expr = F.lit(weights[0])
for label in [1, 2]:
    weight_expr = (
        F.when(F.col("label") == float(label), F.lit(weights[label]))
         .otherwise(weight_expr)
    )

train = train.withColumn("class_weight", weight_expr)

print("Poids des classes :", weights)

print("\n=== 5. CONSTRUCTION DU PIPELINE TF-IDF + LOGISTIC REGRESSION ===")

tokenizer = RegexTokenizer(
    inputCol="comment_text",
    outputCol="words",
    pattern=r"\s+",
    gaps=True,
    minTokenLength=2
)

hashing = HashingTF(
    inputCol="words",
    outputCol="raw_features",
    numFeatures=1 << 18
)

idf = IDF(
    inputCol="raw_features",
    outputCol="features"
)

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    weightCol="class_weight",
    maxIter=30,
    regParam=0.1,
    family="multinomial"
)

pipeline = Pipeline(stages=[tokenizer, hashing, idf, lr])

print("\n=== 6. ENTRAINEMENT ===")
model = pipeline.fit(train)
print("Entrainement termine.")

print("\n=== 7. EVALUATION ===")
predictions = model.transform(test).cache()

for metric in ["accuracy", "f1", "weightedPrecision", "weightedRecall"]:
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName=metric
    )
    print(f"{metric}: {evaluator.evaluate(predictions):.4f}")

print("\n=== F1 PAR CLASSE ===")
for label, name in [(0, "Negatif"), (1, "Neutre"), (2, "Positif")]:
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="fMeasureByLabel",
        metricLabel=float(label)
    )
    print(f"{name}: {evaluator.evaluate(predictions):.4f}")

print("\n=== MATRICE DE CONFUSION ===")
predictions.groupBy("label", "prediction").count().orderBy(
    "label", "prediction"
).show()

print("\n=== 8. SAUVEGARDE DU MODELE ===")
model.write().overwrite().save(MODEL_PATH)
print("Modele sauvegarde :", MODEL_PATH)

predictions.unpersist()
train.unpersist()
df.unpersist()
spark.stop()

print("\n=== SPARK ML TERMINE AVEC SUCCES ===")
