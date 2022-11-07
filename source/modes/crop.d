module modes.crop;

import dpq2;
import frontend: Settings, Mode;
import postgresql: DataBaseAccess;
import csvmanip: FilteredCSV;
import crops: Crops;

/**
 *
 * account_voucher
 * tax_tr
 * shipment_quantity
 * shipment_package
 * sale_price
 * shipment_costs
 * shipment_insentive
 * shipment_info
 *
 */
/+
struct QuantityInfo(Crop){
	this() @safe pure nothrow @nogc{}

	this(in ushort[] nums) @safe pure nothrow @nogc{
		rowQuantityCsv[]= nums[];
	}

	@safe pure nothrow @nogc{
		size_t numOfAllClass() const{
			return _size;
		}

		int quantity(in string str) const{
			int result;
			auto idx= className.countUntil(str);
			if(idx > 0){
				result= rowQuantityCsv[idx];
			}
			else{
				throw new Exception("Error: unknown shipment class");
			}
			return result;
		}
	}

	private:
	static if(Crop is Crops.eggplant){
		size_t _size= 18;
		string[_size] className= ["A-L 8kg", "L 8kg", "B-L 8kg",
													 "A-M 8kg", "M 8kg", "B-M 8kg",
													 "A-S 8kg", "S 8kg", "B-S 8kg",
													 "A-L 4kg", "L 4kg", "B-L 4kg",
													 "A-M 4kg", "M 4kg", "B-M 4kg",
													 "A-S 4kg", "S 4kg", "B-S 4kg"];

	}
	else static if(Crop is Crops.zucchini){
		size_t _size= 10;
		string[_size] className= ["A-2L 2kg", "A-L 2kg", "A-M 2kg", "A-S 2kg", "A-2S 2kg",
													 "B-2L 2kg", "B-L 2kg", "B-M 2kg", "B-S 2kg", "B-2S 2kg"];
	}
	else static if(Crop is Crops.shrinkedSpinach){
		size_t _size= 3;
		string[_size] className= ["A 5kg", "A-circ 5kg", "B 5kg"];
	}
	else static if(Crop is Crops.onion){
		size_t _size= 50;
		string[_size] className= ["A-2L 1t鉄コン", "B-2L 1t鉄コン",
												 "A-L 1t鉄コン", "B-L 1t鉄コン",
												 "A-M 1t鉄コン", "B-M 1t鉄コン",
												 "A-S 1t鉄コン", "B-S 1t鉄コン",
												 "A-2S 1t鉄コン", "B-2S 1t鉄コン",
												 "A-2L 0.5t鉄コン", "B-2L 0.5t鉄コン",
												 "A-L 0.5t鉄コン", "B-L 0.5t鉄コン",
												 "A-M 0.5t鉄コン", "B-M 0.5t鉄コン",
												 "A-S 0.5t鉄コン", "B-S 0.5t鉄コン",
												 "A-2S 0.5t鉄コン", "B-2S 0.5t鉄コン",
												 "A-2L 20kgDB", "B-2L 20kgDB",
												 "A-L 20kgDB", "B-L 20kgDB",
												 "A-M 20kgDB", "B-M 20kgDB",
												 "A-S 20kgDB", "B-S 20kgDB",
												 "A-2S 20kgDB", "B-2S 20kgDB",
												 "A-2L 10kgDB", "B-2L 10kgDB",
												 "A-L 10kgDB", "B-L 10kgDB",
												 "A-M 10kgDB", "B-M 10kgDB",
												 "A-S 10kgDB", "B-S 10kgDB",
												 "A-2S 10kgDB", "B-2S 10kgDB",
												 "A-2L 10kg網", "B-2L 10kg網",
												 "A-L 10kg網", "B-L 10kg網",
												 "A-M 10kg網", "B-M 10kg網",
												 "A-S 10kg網", "B-S 10kg網",
												 "A-2S 10kg網", "B-2S 10kg網"];
	}
	else static if(Crop is Crops.taro){
		size_t _size= 3;
		string[_size] className= ["A-L 5kgDB", "A-M 5kgDB", "A-S 5kgDB"];
	}


	ushort[size] rowQuantityCsv;
}
+/
void registerCropData(Connection conn,
			in Crops crop,
			in Settings spc,
			in FilteredCSV!dstring[2] bufCSV) @system{
	import std.array: appender;
	import std.conv: to;
	import std.csv: csvReader;
	import std.datetime: Date;
	import crops: cropNameStr;
	enum string TR_ID_SEQ_NAME= "list_of_trs_tr_id_seq";
	enum string ACCOUNT_VOUCHER_FMT= "%s%s;%s";
	enum string COMMON_PROCESS= q{
		if(totalMass > 0){
			cmdAddQuantity.args[1]= toValue(classStr[idxCol]);
			cmdAddQuantity.args[2]= totalMass.toValue;
			conn.execParams(cmdAddQuantity);
		}
		else continue;

		{
			import std.math: floor;
			with(cmdGetSale){
				args[0]= dateShipment.toValue;
				args[1]= cropNameStr(crop).toValue;
				args[2]= stationShipment.toValue;
				args[3]= classStr[idxCol].toValue;
			}
			auto ans= conn.execParams(cmdGetSale);
			saleTotal += floor(ans[0]["price_kg"].as!double*totalMass);
		}
	};

// FIXME: const
	string[string][uint] bufQuantity= (in content) @safe pure{
		//import std.algorithm: isStrictlyMonotonic, sort;
		import std.algorithm: remove, countUntil;
		import std.csv: csvReader;
		import std.conv: to;

		string[string] part;
		string[string][uint] result;
		string[] headers;

		auto record= csvReader!(string[string])(content.validData.dup, null);
		headers= record.header.dup;
		headers= headers.remove(headers.countUntil("seq_internal"));

		foreach(scope row; record){
			foreach(label; headers) part[label]= row[label];
			result[row["seq_internal"].to!uint]= part.dup;
		}
/+
		if(rows.isStrictlyMonotonic!(a["seq_internal"] < b["seq_internal"])){
			return rows;
		}
		else{
			return rows.sort!(a["seq_internal"] < b["seq_internal"]);
		}
+/
		return result;
	}(bufCSV[0]);

	const FilteredCSV!dstring bufCost= bufCSV[1];

	int idShipment, idSub, idInternal;
	size_t rowIdxQuantity;
	typeof(appender!string()) bufSummary;
	string[string] rowQuantityCsv;
	Date dateShipment;
	int totalMass;
	double saleTotal;
	string stationShipment;

	@(DataBaseAccess.readonly) QueryParams cmdGetSale;
	@(DataBaseAccess.append) QueryParams cmdAddInfo;
	@(DataBaseAccess.append) QueryParams cmdAddVoucher;
	@(DataBaseAccess.append) QueryParams cmdAddTaxTr;
	@(DataBaseAccess.append) QueryParams cmdAddCosts;
	@(DataBaseAccess.append) QueryParams cmdAddInsentive;
	@(DataBaseAccess.append) QueryParams cmdAddQuantity;
	@(DataBaseAccess.append) QueryParams cmdAddPackage;

	with(cmdGetSale){
		sqlCommand=`SELECT CAST(unit_price*1000.0/unit_mass AS DOUBLE PRECISION) AS price_kg
FROM sale_price
WHERE shipment_date = $1::DATE AND
	crop_name = $2::TEXT AND
	station = $3::TEXT AND
	class_ = $4::VARCHAR(3);`;
		args.length= 4;
	}

	with(cmdAddInfo){
		sqlCommand= `INSERT INTO shipment_info
(shipment_id, shipment_date, reward_id, crop_name) VALUES
($1::INTEGER, $2::DATE, $3::INTEGER, $4::TEXT);`;
		args.length= 4;
	}

	with(cmdAddVoucher){
		sqlCommand= `INSERT INTO account_voucher
(tr_id, sub_id, summary, price, title_debit, title_credit) VALUES
($1::INTEGER, $2::SMALLINT, $3::TEXT, $4::INTEGER, $5::TEXT, $6::TEXT);`;
		args.length= 6;
	}

	with(cmdAddTaxTr){
		sqlCommand= `INSERT INTO tax_tr
(tr_id, tax_name, price, direction) VALUES
($1::INTEGER, $2::TEXT, $3::INTEGER, $4::CHAR);`;
		args.length= 4;
	}

	with(cmdAddQuantity){
		sqlCommand= `INSERT INTO shipment_quantity
(tr_id, class_, nominal_mass) VALUES
($1::INTEGER, $2::VARCHAR(3), $3::SMALLINT);`;
		args.length= 3;
	}

	with(cmdAddPackage){
		sqlCommand= `INSERT INTO shipment_package
(tr_id, package_config, quantity) VALUES
($1::INTEGER, $2::TEXT, $3::SMALLINT);`;
		args.length= 3;
	}

	with(cmdAddCosts){
		sqlCommand= `INSERT INTO shipment_costs
(tr_id, market_fee, ja_fee, fare, insurance) VALUES
($1::INTEGER, $2::SMALLINT, $3::SMALLINT, $4::SMALLINT, $5::SMALLINT);`;
		args.length= 5;
	}

	with(cmdAddInsentive){
		sqlCommand= `INSERT INTO shipment_insentive
(tr_id, sub_id) VALUES
($1::INTEGER, $2::SMALLINT);`;
		args.length= 2;
	}

	foreach(scope record; csvReader!(string[string])(bufCost.validData.dup, null)){
		idSub= 1;
		dateShipment= Date.fromISOExtString(record["shipment[yyyy-MM-dd]"]);
		stationShipment= record["station"];

		// list_of_trs
		{
			import process: commaSepTextToArray, checkEvidenceFile, addTrList;
			string[] refFiles, buf;
			Value[5] rowDataTrs;
			rowDataTrs[0]= toValue(dateShipment);
			rowDataTrs[1]= Value(ValueFormat.BINARY, OidType.Int2);	// null
			rowDataTrs[2]= toValue(stationShipment);
			rowDataTrs[3]= toValue("S");

			refFiles= commaSepTextToArray(record["reference_shipment"]);
			foreach(scope theFname; refFiles){
			buf ~= theFname;
			/+
				if(conn.refFileAlreadyRegistered(theFname)){
					buf ~= theFname;
				}
				else{
					buf ~= checkEvidenceFile(theFname, bufCSV.filename);
				}
				+/
			}
			rowDataTrs[4]= toValue(buf);
			idShipment= conn.addTrList(rowDataTrs);
		}

		cmdAddVoucher.args[0]= toValue(idShipment);

		// shipment_info
		with(cmdAddInfo){
			args[0]= idShipment.toValue;
			args[1]= dateShipment.toValue;
			args[2]= record["seq_reward"].to!int.toValue;
			args[3]= cropNameStr(crop).toValue;
		}
		conn.execParams(cmdAddInfo);

		// sale by class
		{
			import std.stdio: writefln, writeln;
			idInternal= record["seq_internal"].to!uint;
			rowQuantityCsv= bufQuantity[idInternal].dup;
			saleTotal= 0.0;

			cmdAddQuantity.args[0]= toValue(idShipment);
			cmdAddPackage.args[0]= toValue(idShipment);

			final switch(crop){
			case Crops.eggplant:
				enum string[9] classStr= ["AL", "AM", "AS",
																	"L", "M", "S",
																	"BL", "BM", "BS"];
				enum LEN= classStr.length;
				enum string[LEN*2] valueHeader= ["A-L 8kg", "A-M 8kg", "A-S 8kg",
																				"L 8kg", "M 8kg", "S 8kg",
																				"B-L 8kg", "B-M 8kg", "B-S 8kg",
																				"A-L 4kg", "A-M 4kg", "A-S 4kg",
																				"L 4kg", "M 4kg", "S 4kg",
																				"B-L 4kg", "B-M 4kg", "B-S 4kg"];
				enum string[2] packageConfig= ["DB8kg", "DB4kg"];
				uint[2] packageAmount= 0;

				foreach(scope size_t idxCol; 0..LEN){
					totalMass= 8*rowQuantityCsv[valueHeader[idxCol]].to!uint
						+4*rowQuantityCsv[valueHeader[9+idxCol]].to!uint;
					mixin(COMMON_PROCESS);

					packageAmount[0] += rowQuantityCsv[valueHeader[idxCol]].to!uint;	// 8kg
					packageAmount[1] += rowQuantityCsv[valueHeader[9+idxCol]].to!uint;	// 4kg
				}

				if(packageAmount[0]+packageAmount[1] > 0){
					foreach(scope size_t idxPackage; 0..2){
						if(packageAmount[idxPackage] > 0){
							cmdAddPackage.args[1]= toValue(packageConfig[idxPackage]);
							cmdAddPackage.args[2]= toValue(to!string(packageAmount[idxPackage]));
							conn.execParams(cmdAddPackage);
						}
						else continue;
					}
				}
				else{
					writefln!"NOTICE: There are no shipments on internal_id=%d."(idInternal);
				}
				break;
			case Crops.zucchini:
				enum string[10] classStr= ["A2L", "AL", "AM", "AS", "A2S",
																	 "B2L", "BL", "BM", "BS", "B2S"];
				enum string[10] valueHeader= ["A-2L 2kg", "A-L 2kg", "A-M 2kg", "A-S 2kg", "A-2S 2kg",
																			"B-2L 2kg", "B-L 2kg", "B-M 2kg", "B-S 2kg", "B-2S 2kg"];
				enum LEN= classStr.length;
				uint packageAmount= 0;

				cmdAddPackage.args[1]= toValue("DB2kg");

				foreach(scope size_t idxCol; 0..LEN){
					totalMass= 2*rowQuantityCsv[valueHeader[idxCol]].to!uint;
					mixin(COMMON_PROCESS);
					/+
					if(totalMass > 0){
						cmdAddQuantity.args[1]= toValue(classStr[idxCol]);
						cmdAddQuantity.args[2]= totalMass.toValue;
						conn.execParams(cmdAddQuantity);
					}
					else continue;
					+/
					packageAmount += rowQuantityCsv[valueHeader[idxCol]].to!uint;
				}

				if(packageAmount > 0){
					cmdAddPackage.args[2]= toValue(to!string(packageAmount));
					conn.execParams(cmdAddPackage);
				}
				else{
					writefln!"NOTICE: There are no shipments on internal_id=%d."(idInternal);
				}
			break;
			case Crops.shrinkedSpinach:
				enum string[3] classStr= ["A", "Acr", "B"];
				enum string[3] valueHeader= ["A 5kg", "A-circ 5kg", "B 5kg"];
				enum LEN= classStr.length;
				uint packageAmount= 0;

				cmdAddPackage.args[1]= toValue("DB5kg");

				foreach(scope size_t idxCol; 0..LEN){
					totalMass= 5*rowQuantityCsv[valueHeader[idxCol]].to!uint;
					mixin(COMMON_PROCESS);
					packageAmount += rowQuantityCsv[valueHeader[idxCol]].to!uint;
				}

				if(packageAmount > 0){
					cmdAddPackage.args[2]= toValue(to!string(packageAmount));
					conn.execParams(cmdAddPackage);
				}
				else{
					writefln!"NOTICE: There are no shipments on internal_id=%d"(idInternal);
				}
				break;
			case Crops.onion:
				enum string[10] classStr= ["A2L", "AL", "AM", "AS", "A2S",
																	"B2L", "BL", "BM", "BS", "B2S"];
				enum string[10] valueHeader= [
					"A-2L 10kg網", "A-L 10kg網", "A-M 10kg網", "A-S 10kg網", "A-2S 10kg網",
					"B-2L 10kg網", "B-L 10kg網", "B-M 10kg網", "B-S 10kg網", "B-2S 10kg網"];
				enum LEN= classStr.length;
				uint packageAmount= 0;
				cmdAddPackage.args[1]= toValue("網10kg");

				foreach(scope size_t idxCol; 0..LEN){
					totalMass= 10*rowQuantityCsv[valueHeader[idxCol]].to!uint;
					mixin(COMMON_PROCESS);
					packageAmount += rowQuantityCsv[valueHeader[idxCol]].to!uint;
				}

				if(packageAmount > 0){
					cmdAddPackage.args[2]= toValue(to!string(packageAmount));
					conn.execParams(cmdAddPackage);
				}
				else{
					writefln!"NOTICE: There are no shipments on internal_id=%d"(idInternal);
				}
				break;
			case Crops.taro:
				enum string[4] classStr= ["2L", "L", "M", "S"];
				enum string[4] valueHeader= ["2L 5kg", "L 5kg", "M 5kg", "S 5kg"];
				enum LEN= classStr.length;
				uint packageAmount= 0;

				cmdAddPackage.args[1]= toValue("DB5kg");

				foreach(scope size_t idxCol; 0..LEN){
					totalMass= 5*rowQuantityCsv[valueHeader[idxCol]].to!uint;
					mixin(COMMON_PROCESS);
					packageAmount += rowQuantityCsv[valueHeader[idxCol]].to!uint;
				}

				if(packageAmount > 0){
					cmdAddPackage.args[2]= toValue(to!string(packageAmount));
					conn.execParams(cmdAddPackage);
				}
				else{
					writefln!"NOTICE: There are no shipments on internal_id=%d."(idInternal);
				}
			}
		}

		// account_voucher, costs, insentive 
		{
			import std.array: appender;
			import std.format: formattedWrite;
			cmdAddCosts.args[0]= idShipment.toValue;
			cmdAddInsentive.args[0]= idShipment.toValue;

			// 売上高
			bufSummary= appender!string;
			bufSummary.formattedWrite!ACCOUNT_VOUCHER_FMT(record["type"], cropNameStr(crop), "販売");
			with(cmdAddVoucher){
				enum double TAX_RATIO= 1.08;
				import std.math: round;
				args[0]= toValue(idShipment);
				args[1]= toValue(idSub++);
				args[2]= toValue(bufSummary.data);
				args[3]= toValue(round(TAX_RATIO*saleTotal).to!int);
				args[4]= toValue("売掛金");
				args[5]= toValue("製品売上高");
			}
			conn.execParams(cmdAddVoucher);

			// 市場手数料
			bufSummary= appender!string;
			bufSummary.formattedWrite!ACCOUNT_VOUCHER_FMT(record["type"], cropNameStr(crop), "市場手数料");
			with(cmdAddVoucher){
				args[0]= toValue(idShipment);
				args[1]= toValue(idSub);
				args[2]= toValue(bufSummary.data);
				args[3]= toValue(record["市場手数料"]);
				args[4]= toValue("販売手数料");
				args[5]= toValue("売掛金");
			}
			conn.execParams(cmdAddVoucher);
			cmdAddCosts.args[1]= toValue(idSub);
			++idSub;

			// 農協手数料
			bufSummary= appender!string;
			bufSummary.formattedWrite!ACCOUNT_VOUCHER_FMT(record["type"], cropNameStr(crop), "JA手数料");
			with(cmdAddVoucher){
				args[0]= toValue(idShipment);
				args[1]= toValue(idSub);
				args[2]= toValue(bufSummary.data);
				args[3]= toValue(record["農協手数料"]);
				args[4]= toValue("販売手数料");
				args[5]= toValue("売掛金");
			}
			conn.execParams(cmdAddVoucher);
			cmdAddCosts.args[2]= toValue(idSub);
			++idSub;

			// 運賃
			bufSummary= appender!string;
			bufSummary.formattedWrite!ACCOUNT_VOUCHER_FMT(record["type"], cropNameStr(crop), "運賃");
			with(cmdAddVoucher){
				args[0]= toValue(idShipment);
				args[1]= toValue(idSub);
				args[2]= toValue(bufSummary.data);
				args[3]= toValue(record["運賃"]);
				args[4]= toValue("荷造運賃");
				args[5]= toValue("売掛金");
			}
			conn.execParams(cmdAddVoucher);
			cmdAddCosts.args[3]= toValue(idSub);
			++idSub;

			// 保険負担金
			bufSummary= appender!string;
			bufSummary.formattedWrite!ACCOUNT_VOUCHER_FMT(record["type"], cropNameStr(crop), "保険負担金");
			with(cmdAddVoucher){
				args[0]= toValue(idShipment);
				args[1]= toValue(idSub);
				args[2]= toValue(bufSummary.data);
				args[3]= toValue(record["保険負担金"]);
				args[4]= toValue("共済掛金");
				args[5]= toValue("売掛金");
			}
			conn.execParams(cmdAddVoucher);
			cmdAddCosts.args[4]= toValue(idSub);
			++idSub;

			// 出荷奨励金
			bufSummary= appender!string;
			bufSummary.formattedWrite!ACCOUNT_VOUCHER_FMT(record["type"], cropNameStr(crop), "出荷奨励金");
			with(cmdAddVoucher){
				args[0]= toValue(idShipment);
				args[1]= toValue(idSub);
				args[2]= toValue(bufSummary.data);
				args[3]= toValue(record["出荷奨励金"]);
				args[4]= toValue("売掛金");
				args[5]= toValue("一般助成収入");
			}
			conn.execParams(cmdAddVoucher);
			cmdAddInsentive.args[1]= toValue(idSub);

			conn.execParams(cmdAddCosts);
			conn.execParams(cmdAddInsentive);
		}

		// tax_tr 売上の消費税
		with(cmdAddTaxTr){
			args[0]= toValue(idShipment);
			args[1]= toValue("消費税");
			args[2]= toValue(record["tax_sale"]);
			args[3]= toValue("I");
		}
		conn.execParams(cmdAddTaxTr);

		// tax_tr 出荷経費の消費税
		with(cmdAddTaxTr){
			args[0]= toValue(idShipment);
			args[1]= toValue("消費税");
			args[2]= toValue(record["tax_cost"]);
			args[3]= toValue("O");
		}
		conn.execParams(cmdAddTaxTr);
	}
}
