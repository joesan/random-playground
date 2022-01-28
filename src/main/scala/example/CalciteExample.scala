package example

import org.apache.calcite.DataContext
import org.apache.calcite.config.{CalciteConnectionConfigImpl, CalciteConnectionProperty}
import org.apache.calcite.jdbc.{CalciteSchema, JavaTypeFactoryImpl}
import org.apache.calcite.linq4j.{Enumerable, Linq4j}
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.ScannableTable
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.validate.{SqlValidator, SqlValidatorUtil}
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.sql.SqlExplainFormat
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.sql2rel.StandardConvertletTable
import org.apache.calcite.plan.ConventionTraitDef
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rex.RexBuilder

import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._

object CalciteExample extends Greeting with App {

  trait Functor[F[_]] {
    def map[A, B](f: A => B)(fa: F[A]): F[B]
  }

  def newCluster(factory: RelDataTypeFactory) = {
    val planner = new VolcanoPlanner
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE)
    RelOptCluster.create(planner, new RexBuilder(factory))
  }

  class ListTable(rowType: RelDataType, data: List[Array[Any]]) extends AbstractTable with ScannableTable {
    override def scan(root: DataContext): Enumerable[Array[Any]] = Linq4j.asEnumerable(data.asJava)
    override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = rowType
  }

  val books: List[Array[Any]] = List(
    Array[Any](1, "Les Miserables", 1862, 2),
    Array[Any](2, "The Hunchback of Notre-Dame", 1829, 2),
    Array[Any](3, "The Last Day of a Condemned Man", 1829, 1),
    Array[Any](4, "The three Musketeers", 1844, 2),
    Array[Any](5, "The Count of Monte Cristo", 1884, 1),
    Array[Any](6, "The Blockchain", 1899, 1)
  )

  val authors = List(
    Array[Any](1, "Joe 1", "San 1"),
    Array[Any](2, "Joe 2", "San 2")
  )

  // Instantiate a type factory for creating types (e.g., VARCHAR, NUMERIC, etc.)
  val typeFactory = new JavaTypeFactoryImpl()

  // Create the root schema describing the data model
  val schema = CalciteSchema.createRootSchema(true)

  // Define type for authors table
  val authorType = new RelDataTypeFactory.Builder(typeFactory)
  authorType.add("id", SqlTypeName.INTEGER)
  authorType.add("fname", SqlTypeName.VARCHAR)
  authorType.add("lname", SqlTypeName.VARCHAR)

  // Initialize authors table with data
  val authorsTable = new ListTable(authorType.build(), authors);
  // Add authors table to the schema
  schema.add("author", authorsTable);

  // Define type for books table
  val bookType = new RelDataTypeFactory.Builder(typeFactory)
  bookType.add("id", SqlTypeName.INTEGER)
  bookType.add("title", SqlTypeName.VARCHAR)
  bookType.add("year", SqlTypeName.VARCHAR)
  bookType.add("author", SqlTypeName.INTEGER)

  // Initialize books table with data
  val booksTable = new ListTable(bookType.build(), books);
  // Add authors table to the schema
  schema.add("book", booksTable);

  // Create a parser for the SQL query
  val parser = SqlParser.create(
    "SELECT b.id, b.title, b.\"year\", a.fname || ' ' || a.lname \n"
      + "FROM Book b\n"
      + "LEFT OUTER JOIN Author a ON b.author=a.id\n"
      + "WHERE b.\"year\" > 1830\n"
      + "ORDER BY b.id\n"
      + "LIMIT 5")

  // Parse the query into an AST
  val sqlNode = parser.parseQuery

  // Configure and instantiate validator
  val props = new Properties();
  props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
  val config = new CalciteConnectionConfigImpl(props);
  val catalogReader = new CalciteCatalogReader(schema,
    Collections.singletonList(""),
    typeFactory, config);

  val validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
    catalogReader, typeFactory,
    SqlValidator.Config.DEFAULT);

  // Validate the initial AST
  val validNode = validator.validate(sqlNode)

  // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
  val NOOP_EXPANDER = (rowType, queryString, schemaPath, viewPath) -> null
  val cluster = newCluster(typeFactory)
  val relConverter = new SqlToRelConverter(NOOP_EXPANDER, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, SqlToRelConverter.config)

  // Convert the valid AST into a logical plan
  val logPlan = relConverter.convertQuery(validNode, false, true).rel

  // Display the logical plan
  System.out.println(RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES))
}

trait Greeting {
  lazy val greeting: String = "hello"
}
