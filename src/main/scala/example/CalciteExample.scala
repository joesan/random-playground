package example

import org.apache.calcite.DataContext
import org.apache.calcite.jdbc.{CalciteSchema, JavaTypeFactoryImpl}
import org.apache.calcite.linq4j.{Enumerable, Linq4j}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.ScannableTable
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.`type`.SqlTypeName

import scala.jdk.CollectionConverters._

object Hello extends Greeting with App {

  class ListTable[T](rowType: RelDataType, data: Seq[T]) extends AbstractTable with ScannableTable {
    override def scan(root: DataContext): Enumerable[Array[AnyRef]] = Linq4j.asEnumerable(data.asJava)
    override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = rowType
  }

  val books = Seq(
    (1, "Les Miserables", 1862, 2),
    (2, "The Hunchback of Notre-Dame", 1829, 2),
    (3, "The Last Day of a Condemned Man", 1829, 1),
    (4, "The three Musketeers", 1866, 2),
    (5, "The Count of Monte Cristo", 1880, 1),
    (6, "The Blockchain", 1899, 1),
  )

  val authors = Seq(
    (1, "Joe 1", "San 1"),
    (2, "Joe 2", "San 2")
  )

  // Instantiate a type factory for creating types (e.g., VARCHAR, NUMERIC, etc.)
  val typeFactory = new JavaTypeFactoryImpl()

  // Create the root schema describing the data model
  val schema = CalciteSchema.createRootSchema(true)

  // Define type for authors table
  val authorType = new RelDataTypeFactory.Builder(typeFactory)
  authorType.add("id", SqlTypeName.INTEGER)
  authorType.add("first_name", SqlTypeName.VARCHAR)
  authorType.add("last_name", SqlTypeName.VARCHAR)

  // Initialize authors table with data
  val authorsTable = new ListTable(authorType.build(), authors);
  // Add authors table to the schema
  schema.add("author", authorsTable);

  // Define type for books table
  val bookType = new RelDataTypeFactory.Builder(typeFactory)
  bookType.add("id", SqlTypeName.INTEGER)
  bookType.add("book_title", SqlTypeName.VARCHAR)
  bookType.add("year", SqlTypeName.VARCHAR)
  bookType.add("author", SqlTypeName.INTEGER)

  // Initialize books table with data
  val booksTable = new ListTable(bookType.build(), books);
  // Add authors table to the schema
  schema.add("book", booksTable);
}

trait Greeting {
  lazy val greeting: String = "hello"
}