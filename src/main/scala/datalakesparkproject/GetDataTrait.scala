package datalakesparkproject
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql._
import java.util.Properties
import java.io.FileInputStream
trait GetDataTrait {
  def getRdbmsData(sqlc: org.apache.spark.sql.SQLContext,DatabaseName: String, TableName: String, PartitionColumn:String, 
      ConnFile: String,upperbound:Long,partflag:Boolean): DataFrame= {

val conn=new Properties()
val propFile= new FileInputStream(s"$ConnFile") 
conn.load(propFile)

/* Reading mysql server connection detail from property file */
val Driver=conn.getProperty("driver") 
val Host =conn.getProperty("host") 
val Port =conn.getProperty("port") 
val User =conn.getProperty("user") 
val Pass =conn.getProperty("pass")

val url="jdbc:mysql://"+Host+":"+Port+"/"+s"$DatabaseName"
if(partflag==true)
{
sqlc.read.format("jdbc")
.option("driver",s"$Driver")
.option("url",s"$url")
.option("user",s"$User")
.option("lowerBound",1)
.option("upperBound",s"$upperbound")
.option("numPartitions",4)
.option("partitionColumn",s"$PartitionColumn")
.option("password",s"$Pass")
.option("dbtable",s"$TableName").load()
}
else
{
sqlc.read.format("jdbc")
.option("driver",s"$Driver")
.option("url",s"$url")
.option("user",s"$User")

.option("password",s"$Pass")
.option("dbtable",s"$TableName").load()
  }  
}
  

}