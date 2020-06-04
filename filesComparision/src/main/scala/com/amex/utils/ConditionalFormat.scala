package main.scala.com.amex.utils

import org.apache.commons.lang3.math.NumberUtils
import org.apache.poi.hssf.usermodel.{HSSFSheet, HSSFWorkbook}
import org.apache.poi.ss.usermodel.CellType

import scala.collection.mutable.ListBuffer

class ConditionalFormat {
  def createExcelWorkbook(hwb:HSSFWorkbook, data:ListBuffer[ListBuffer[String]]): Int ={


    var i = 0
    val sheet = hwb.createSheet("Sheet1")
    data.foreach(line => {
      val row = sheet.createRow(i)
      var j=0
      line.foreach(f => {
        val cell = row.createCell(j)
        val field = f.trim

        if (NumberUtils.isCreatable(field.trim())) {
          cell.setCellType(CellType.NUMERIC)
          cell.setCellValue(java.lang.Double.parseDouble(field))
        }else if (field.endsWith("%") && NumberUtils.isCreatable(field.substring(0, field.lastIndexOf('%')))) {
          val field1 = field.substring(0, field.lastIndexOf('%'))
          cell.setCellType(CellType.NUMERIC)
          cell.setCellValue(Math.round(java.lang.Double.parseDouble(field1) * 100.0) / 100.0)
          //                        style.setDataFormat(hwb.createDataFormat().getFormat(BuiltinFormats.getBuiltinFormat( 10 ))); //"0.00%"
        }else if (field.startsWith("=")) {
          cell.setCellType(CellType.STRING)
          cell.setCellValue(field.substring(1, field.length))
        }else {
          cell.setCellType(CellType.STRING)
          cell.setCellValue(field)
        }
        j+=1
      })
      i+=1
    })

    System.out.println("Total number of columns: " + (i - 1))
    System.out.println("Excel file creation has completed. Conditional formatting started...")
    i
  }

  import org.apache.poi.ss.util.CellReference

  import util.control.Breaks._

  def getConditionalWorkbook(hwb: HSSFWorkbook, rowCount: Int): HSSFWorkbook ={
    val sheet = hwb.getSheet("Sheet1")
    var currentCol = 7
    breakable {
      while(true) {
        val cell = sheet.getRow(1).getCell(currentCol)
        if (cell != null) {
          val columnLetter = CellReference.convertNumToColString(currentCol)
          val range = columnLetter + "1:" + columnLetter + "" + rowCount
          colourScales(sheet, range)
          System.out.println("processing for column " + columnLetter + " and data range is " + range)
          currentCol += 5
        }else
          break

        if (currentCol > 50) break  // break out of the for loop
      }
    }

    hwb
  }

  import org.apache.poi.ss.usermodel.ConditionalFormattingThreshold.RangeType
  import org.apache.poi.ss.usermodel.ExtendedColor
  import org.apache.poi.ss.util.CellRangeAddress

  def colourScales(sheet: HSSFSheet, range: String): Unit = {
      val sheetCF = sheet.getSheetConditionalFormatting()
      val regions = Array(CellRangeAddress.valueOf(range))
      val rule1 = sheetCF.createConditionalFormattingColorScaleRule
      val cs1 = rule1.getColorScaleFormatting
      cs1.getThresholds()(0).setRangeType(RangeType.MIN)
      cs1.getThresholds()(1).setRangeType(RangeType.PERCENTILE)
      cs1.getThresholds()(1).setValue(50d)
      cs1.getThresholds()(2).setRangeType(RangeType.MAX)
      // Red-Yellow-Green
      cs1.getColors()(0).asInstanceOf[ExtendedColor].setARGBHex("FFF8696B")
      cs1.getColors()(1).asInstanceOf[ExtendedColor].setARGBHex("FFFFEB84")
      cs1.getColors()(2).asInstanceOf[ExtendedColor].setARGBHex("FF63BE7B")
      sheetCF.addConditionalFormatting(regions, rule1)
    }


}
