from pyspark.sql import SparkSession, functions as sf
from pyspark.sql.types import IntegerType
import math
import sys

bucket = sys.argv[1]
fn = f"gs://{bucket}/fastq.csv"
spark = SparkSession.builder.getOrCreate()

df = spark.read.option("header",True)\
    .option("delimiter","|")\
    .csv(fn).cache()

#col_name: column using for analysis (using the first column if not specified)
#max_len: number of reads using for analysis (using all reads if not specified)
class Common:
    def __init__(self, df, col_name, max_len):
        self.df=df
        self.col_name = col_name if col_name else df.columns[0]
        self.mxlen = max_len if max_len else df.select(sf.length(self.col_name)).groupby().max().first()[0]

#total_seq: the first N sequences using for analysis (using all sequences if not specified)
class TotalSeq:
    def gettotalseq(self, total_seq):
        self.totalseq = total_seq if total_seq else df.count()

# Input
# col_name: refer to class Common
# max_len: refer to class Common
# total_seq: refer to class TotalSeq

# Output
# total_seq(): number of sequences in analysis
# total_bases(): number of bases (total reads) in analysis
# len_dist(): distribution of sequnce lengths over all sequences -> [{'count': occurrence, 'length': sequnce length},...]
# min_len(): minimum number of reads in a single sequence
# max_len(): maximum number of reads in a single sequence
class Basics(Common,TotalSeq):
    def __init__(self, df, col_name=None, max_len=None, total_seq=None):
        super().__init__(df, col_name, max_len)
        super().gettotalseq(total_seq)
        self.df_len_grp = df.select([sf.length(self.col_name).alias("length")])
    def total_seq(self):
        return self.totalseq
    def total_bases(self):
        return self.df_len_grp.select(sf.sum("length")).first()[0]
    def len_dist(self):
        return self.df_len_grp.groupby("length").count().toPandas().to_dict(orient="records")
    def min_len(self):
        return self.df_len_grp.groupby().min().first()[0]
    def max_len(self):
        return self.mxlen


# Input
# col_name: refer to class Common
# max_len: refer to class Common
# ascii_shift: the quality score encode (default phred + 33)

# Output
# qual_anlys(): Return quality analysis per read position-> [{"label":position of the read,"mean":mean of the quality, "med":median of the quality,"q1":25% quantile of the quality,"q3":75% quantile of the quality,"whishi":10% quantile of the quality,"whislo":90% quantile of the quality},...]
# perseq_anlys(): Return average quality per sequnces->[{"count":count ,"quality": the average quality of a sequence (truncated)},...]

class Quals(Common):
    def __init__(self, df, col_name=None, max_len=None, ascii_shift="!"):
        super().__init__(df, col_name, max_len)
        self.ascii_shift = ord(ascii_shift)
    def qual_anlys(self):
        ascii_shift = self.ascii_shift
        required_quantiles = {"whislo": 0.1, "q1": 0.25, "med": 0.5, "q3": 0.75, "whishi": 0.9}
        df = self.df.select(
            sf.posexplode(sf.split(self.col_name, "")).alias("label", "phred")
        ) \
            .filter('phred != ""').selectExpr(f"label + 1 as label", f"ascii(phred) - {ascii_shift} as quality") \
            .groupBy("label") \
            .agg(
            sf.avg('quality').alias('mean')
            , sf.percentile_approx("quality", list(required_quantiles.values())).alias('quantiles')
        ) \
            .select(
            sf.col("label")
            , sf.col("mean")
            ,
            *[sf.col("quantiles").getItem(idx).alias(name) for idx, name in enumerate(list(required_quantiles.keys()))]
        ) \
            .toPandas()
        return df.to_dict(orient="records")
    def perseq_anlys(self):
        ascii_shift = self.ascii_shift
        ascii_udf = sf.udf(lambda s: math.trunc(sum(ord(c) for c in s)/len(s)-ascii_shift),IntegerType())
        df_ascii = self.df.withColumn('ascii_avg',ascii_udf(sf.col(self.col_name))).groupby("ascii_avg").count().toPandas()
        df_ascii = df_ascii.rename(columns={'ascii_avg': 'quality'})
        return df_ascii.to_dict(orient="records")

# Input
# col_name: refer to class Common
# max_len: refer to class Common
# total_seq: refer to class TotalSeq
# max_sub: First X number of reads for duplication analysis

# Output
# seq_anlys(): return a list of sequence content across all bases ->[{"A":fraction of "A" readings,"C":fraction of "C" readings,"G":fraction of "G" readings,"N":fraction of "N" readings,"T":fraction of "T" readings,"read":position of read},...]
# dupl_count(): return a list of overrepresented sequences (descending order) -> [{"count":occurance of sequence,"percentage of total":occurance of sequence in percentage,"sequence": read of sequence},..]

class Seqs(Common,TotalSeq):
    def __init__(self, df, col_name=None, max_len=None, total_seq=None, max_sub=None):
        super().__init__(df, col_name, max_len)
        super().gettotalseq(total_seq)
        if not max_sub:
            self.df_count = df.limit(self.totalseq).groupby(self.col_name).count()
        else:
            self.df_count = df.limit(self.totalseq).select(sf.substring(self.col_name,1, max_sub).alias(self.col_name)).groupby(self.col_name).count()
    def seq_anlys(self):
        base_list = ["A", "C", "G", "T"]

        df = self.df.select(
            sf.posexplode(sf.split(self.col_name, "")).alias("read", "base")
        ) \
            .filter('base != ""') \
            .groupBy("read") \
            .agg(
            *[sf.count(sf.when(sf.col("base") == base, True)).alias(base) for base in base_list]
            , sf.count(sf.when(sf.col("base") == "N", True)).alias('N')
        ) \
            .select(
            (sf.col('read') + sf.lit(1)).alias('read')
            , *[(sf.col(base) / sum([sf.col(base) for base in base_list])).alias(base) for base in base_list]
            , (sf.col("N") / sum([sf.col(base) for base in base_list + ["N"]])).alias("N")
        ) \
            .toPandas()
        return df.to_dict(orient="records")
    def dupl_count(self):
        df = self.df_count.orderBy(sf.col("count").desc()).limit(20).toPandas()
        df['percentage of total'] = df['count']/self.totalseq
        df = df.to_dict(orient="records")
        return df

result={"basic":{}, "qual":{}, "seq":{}}
basic_stat = Basics(df.select("quality"))
result['basic']['total_seq']=basic_stat.total_seq()
result['basic']['total_bases']=basic_stat.total_bases()
result['basic']['len_dist']=basic_stat.len_dist()
result['basic']['min_len']=basic_stat.min_len()
result['basic']['max_len']=basic_stat.max_len()
print("Completed basic statistic analysis")

qual_stat = Quals(df.select("quality"),"quality",result['basic']['max_len'])
result['qual']['PerBase']=qual_stat.qual_anlys()
result['qual']['PerSeq']=qual_stat.perseq_anlys()
print("Completed quality statistic analysis")

seq_stat = Seqs(df.select("sequence"),"sequence",result['basic']['max_len'],1000000,50)
result['seq']['content']=seq_stat.seq_anlys()
result['seq']['count']=seq_stat.dupl_count()
print("Completed sequence statistic analysis")

spark.read.json(spark.sparkContext.parallelize([result])).coalesce(1).write.mode('overwrite').json(f"gs://{bucket}/output")
