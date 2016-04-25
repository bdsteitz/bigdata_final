import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset
import org.apache.spark.mllib.fpm.FPGrowth

val codesText = sc.textFile("hdfs:/user/goldstm/grad_students/patientcodes")
val outcomes = sc.textFile("hdfs:/user/steitzb/grad_students/patient_outcome/pde.txt")
outcomes.cache()
codesText.cache()

//split by months
var codes1 = codesText.map(s => s.split("\t")).filter(s => s(2).startsWith("201001")).map(s => (s(0),s(3)))
var codes2 = codesText.map(s => s.split("\t")).filter(s => s(2).startsWith("201002")).map(s => (s(0),s(3)))
var codes3 = codesText.map(s => s.split("\t")).filter(s => s(2).startsWith("201003")).map(s => (s(0),s(3)))
var codes4 = codesText.map(s => s.split("\t")).filter(s => s(2).startsWith("201004")).map(s => (s(0),s(3)))
var codes5 = codesText.map(s => s.split("\t")).filter(s => s(2).startsWith("201005")).map(s => (s(0),s(3)))
var codes6 = codesText.map(s => s.split("\t")).filter(s => s(2).startsWith("201006")).map(s => (s(0),s(3)))
var codes7 = codesText.map(s => s.split("\t")).filter(s => s(2).startsWith("201007")).map(s => (s(0),s(3)))
var codes8 = codesText.map(s => s.split("\t")).filter(s => s(2).startsWith("201008")).map(s => (s(0),s(3)))
var codes9 = codesText.map(s => s.split("\t")).filter(s => s(2).startsWith("201009")).map(s => (s(0),s(3)))
var codes10 = codesText.map(s => s.split("\t")).filter(s => s(2).startsWith("201010")).map(s => (s(0),s(3)))
var codes11 = codesText.map(s => s.split("\t")).filter(s => s(2).startsWith("201011")).map(s => (s(0),s(3)))
var codes12 = codesText.map(s => s.split("\t")).filter(s => s(2).startsWith("201012")).map(s => (s(0),s(3)))

var patients1 = (codes1 ++ codes2 ++ codes3 ++ codes4 ++ codes5 ++ codes6).groupByKey()
var patients2 = (codes2 ++ codes3 ++ codes4 ++ codes5 ++ codes6 ++ codes7).groupByKey()
var patients3 = (codes3 ++ codes4 ++ codes5 ++ codes6 ++ codes7 ++ codes8).groupByKey()
var patients4 = (codes4 ++ codes5 ++ codes6 ++ codes7 ++ codes8 ++ codes9).groupByKey()
var patients5 = (codes5 ++ codes6 ++ codes7 ++ codes8 ++ codes9 ++ codes10).groupByKey()
var patients6 = (codes6 ++ codes7 ++ codes8 ++ codes9 ++ codes10 ++ codes11).groupByKey()
var patients7 = (codes7 ++ codes8 ++ codes9 ++ codes10 ++ codes11 ++ codes12).groupByKey()
var patients = patients1 ++ patients2 ++ patients3 ++ patients4 ++ patients5 ++ patients6 ++ patients7

patients.cache()

val fpg = new FPGrowth().setMinSupport(0.001).setNumPartitions(40)
val model = fpg.run(patients.map(s => s._2))
val groups = model.freqItemsets.collect().map(i => (i.items,i.freq))


val minConfidence = 0.4
val associationRules = model.generateAssociationRules(minConfidence).collect().filter(p => p.consequent(0) == "Dead")
).sortWith(_.confidence > _.confidence )

sc.parallelize(associationRules.map(rule =>  rule.antecedent.mkString(",")
  + "\t" + rule.consequent(0)
    + "\t" + rule.confidence) ).saveAsTextFile("hdfs:/user/goldstm/grad_students/rules")
