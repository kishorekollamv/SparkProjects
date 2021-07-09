package datalakesparkproject
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql._
import java.util.Properties
import java.io.FileInputStream
trait WriteDataTrait {
 
def writeHiveTable(df:DataFrame,tblname:String,partflag:Boolean,partcols:String):Boolean= {
if(partflag==false)
{
  df.write.mode(SaveMode.Overwrite).saveAsTable(tblname)
  
}
else
{
  df.write.mode(SaveMode.Overwrite).partitionBy(partcols).saveAsTable(tblname)  
}
true
}

def writeRdbmsData(df:DataFrame,Tablename:String,
    DatabaseName: String, ConnFile: String,writemode:String)= {

// Creating connection object by loading the connection property file 
val conn=new Properties()
val propFile= new FileInputStream(s"$ConnFile") 
conn.load(propFile)

// Reading mysql server connection detail from property file 
val Driver=conn.getProperty("driver") 
val Host =conn.getProperty("host") 
val Port =conn.getProperty("port") 
val User =conn.getProperty("user") 
val Pass =conn.getProperty("pass")
val url="jdbc:mysql://"+Host+":"+Port+"/"+s"$DatabaseName"

// Setting the property
val prop=new java.util.Properties();
prop.put("user", User)
prop.put("password", Pass)
prop.put("driver",Driver)

df.write.mode(writemode).jdbc(url,Tablename,prop)

}

def writeFile(df:DataFrame,fileformat:String,location:String,partflag:Boolean,partcols:String):Boolean= {
 if(partflag==false)
{
   if(fileformat=="json")
   {
  df.write.mode("overwrite").json(location)
   }
   else if(fileformat=="csv")
   {
  df.write.mode("overwrite").option("header",true).csv(location)
   } 
}
else
{
     if(fileformat=="json")
   {
  df.write.mode("overwrite").partitionBy(partcols).json(location)
   }
   else if(fileformat=="csv")
   {
  df.write.mode("overwrite").option("header",true).partitionBy(partcols).csv(location)
   }   
} 
true  
}

def writeToCassandra(df:DataFrame,TableName:String,Keyspace:String)={
import org.apache.spark.sql.SaveMode;
df.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> TableName, "keyspace" -> Keyspace)).mode(SaveMode.Overwrite).save()
}

}