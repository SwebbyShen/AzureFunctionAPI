using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Net;
using System.Net.Http.Headers;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Xml;
using Azure.Identity;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.FileSystemGlobbing;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Microsoft.Identity.Client;
using Microsoft.VisualBasic;
using Newtonsoft.Json;
using static System.Runtime.CompilerServices.RuntimeHelpers;
using static System.Runtime.InteropServices.JavaScript.JSType;
using static billoneexactapi.Function1;
using Prompt = Microsoft.Identity.Client.Prompt;

namespace billoneexactapi
{
    public class Function1
    {
        //private readonly IConfiguration _configuration;
        private readonly ILogger _logger;

        public Function1(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<Function1>();
            //_configuration = configuration;
        }

        [Function("Sample")]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post",Route = "Sample")] HttpRequestData req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(new { 
              Name = "Sample update by newton",
              CurrentTime = DateTime.UtcNow
            });

            return response;
        }
        public class GetInvoiceType
        {
            public string companycode { get; set; }
            public string invoiceidfrom { get; set; }
            public string invoiceidto { get; set; }
            public DateTime invoicedatefrom { get; set; }
            public DateTime invoicedateto { get; set; }
            public int invoiceamountfrom { get; set; }
            public int invoiceamountto { get; set; }
            public string creditorname { get; set; }
            public int registerstatus { get; set; }

        }

        [Function("GetInvoiceList")]
        public async Task<HttpResponseData> GetInvoiceList([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "GetInvoiceList")] HttpRequestData req)
        {
            // requestbodyの内容を読み取る
            string requestBody = await req.ReadAsStringAsync();
            dynamic requestData = JsonConvert.DeserializeObject<GetInvoiceType>(requestBody);

            string companyCode = requestData.companycode;
            string invoiceIdFrom = requestData.invoiceidfrom;
            string invoiceIdTo = requestData.invoiceidto;
            DateTime invoiceDateFrom = requestData.invoicedatefrom;
            DateTime invoiceDateTo = requestData.invoicedateto;
            int? invoiceAmountFrom = requestData.invoiceamountfrom;
            int? invoiceAmountTo = requestData.invoiceamountto;
            string creditorName = requestData.creditorname;
            int? registerStatus = requestData.registerstatus;

            // SQLクエリのwhereの構成
            string whereClause = "";

            if (!string.IsNullOrEmpty(invoiceIdFrom) && !string.IsNullOrEmpty(invoiceIdTo))
            {
                whereClause += " AND ( inv.invoiceid BETWEEN '" + invoiceIdFrom + "' and '" + invoiceIdTo + "')";
            }

            if (!string.IsNullOrEmpty(invoiceIdTo) && string.IsNullOrEmpty(invoiceIdFrom))
            {
                whereClause += " AND inv.invoiceid ='" + invoiceIdTo + "'";
            }

            if (string.IsNullOrEmpty(invoiceIdTo) && !string.IsNullOrEmpty(invoiceIdFrom))
            {
                whereClause += " AND inv.invoiceid ='" + invoiceIdFrom + "'";
            }


            if (invoiceDateFrom != default && invoiceDateTo != default)
            {
                whereClause += " AND ( inv.invoicedate BETWEEN '" + invoiceDateFrom + "' and '" + invoiceDateTo + "')";
            }
            if (invoiceDateTo != default && invoiceDateFrom == default)
            {
                whereClause += " AND inv.invoicedate ='" + invoiceDateTo + "'";
            }
            if (invoiceDateTo == default && invoiceDateFrom != default)
            {
                whereClause += " AND inv.invoicedate ='" + invoiceDateFrom + "'";
            }


            if (invoiceAmountFrom!=0 && invoiceAmountTo != 0)
            {
                whereClause += " AND (inv.invoiceamt BETWEEN '" + invoiceAmountFrom + "' and '" + invoiceAmountTo + "')";
            }
            if (invoiceAmountTo != 0 && invoiceAmountFrom == 0)
            {
                whereClause += " AND inv.invoiceamt = " +invoiceAmountTo;
            }
            if (invoiceAmountTo == 0 && invoiceAmountFrom != 0)
            {
                whereClause += " AND inv.invoiceamt = " + invoiceAmountFrom;
            }

            if (!string.IsNullOrEmpty(creditorName))
            {
                whereClause += " AND inv.creditorname ='" + creditorName + "'";
            }
            if (registerStatus != 0)
            {
                whereClause += " AND inv.registerstatus= " + registerStatus;
            }

            //JSONファイル読み取る
            var config = new ConfigurationBuilder()
            .SetBasePath(Environment.CurrentDirectory)
            .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();

            string sql = "SELECT [id]," +
                "[invoiceid]," +
                "[creditorname]," +
                "CONVERT(VARCHAR(10),[invoicedate],120) as invoicedate," +
                "[invoiceamt]," +
                "[registerstatus]" +
                " FROM billoneinvoices inv WITH(nolock)" +
                " LEFT JOIN(SELECT Max(maincompanycode) AS maincompanycode,employeecode"+
                " FROM view_alldigiuser WITH (nolock)" +
                " GROUP BY employeecode)usr" +
                " ON inv.employeecode = usr.employeecode" +
                " WHERE usr.maincompanycode = '" + companyCode + "'" + whereClause;

            string connectionString = config.GetConnectionString("defaultdatabase");

            _logger.LogInformation($"Connection string: {connectionString}");
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            DataTable dataTable = new DataTable();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand command = new SqlCommand(sql, connection))
                {
                    connection.Open();
                    SqlDataReader reader = command.ExecuteReader();
                    dataTable.Load(reader);
                    reader.Close();
                }
            }

            var response = req.CreateResponse(HttpStatusCode.OK);

            List<object> rows = new List<object>();
            foreach (DataRow row in dataTable.Rows)
            {
                var rowData = new Dictionary<string, object>();
                foreach (DataColumn column in dataTable.Columns)
                {
                    rowData[column.ColumnName] = row[column];
                }
                rows.Add(rowData);
            }
            await response.WriteAsJsonAsync(rows);

            return response;
        }


        [Function("GetJournalList")]
        public async Task<HttpResponseData> GetJournalList([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "GetJournalList")] HttpRequestData req)
        {
            // requestbodyの内容を読み取る
            string requestBody = await req.ReadAsStringAsync();
            dynamic requestData = JsonConvert.DeserializeObject<GetInvoiceType>(requestBody);

            string companyCode = requestData.companycode;

            //JSONファイル読み取る
            var config = new ConfigurationBuilder()
            .SetBasePath(Environment.CurrentDirectory)
            .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();

            string sql = "SELECT dagbk.dagbknr as [journal]," +
                "dagbk.afk as [abbreviation]," +
                "dagbk.oms25_0 as [description]," +
                "Count(amutak.volgnr5) AS [entries]," +
                " CONVERT(VARCHAR(10),ISNULL(max(amutak.docdate),''),120) AS [lastentry],ISNULL((" +
                " CASE" +
                " WHEN type_dgbk IN ('K','G','B')" +
                " THEN sum(amutak.eindsaldo - amutak.beginsaldo)" +
                " ELSE sum(amutak.bedrag)" +
                " END),0) AS [total]" +
                " FROM dagbk WITH(nolock)" +
                " LEFT OUTER JOIN amutak WITH (nolock)" +
                " ON(" +
                " amutak.dagbknr = dagbk.dagbknr" +
                " AND amutak.status IN ('P','E')" +
                " AND amutak.dagbknr IN (' 60',' 67',' 64')" +
                " AND amutak.entrytype NOT IN('R'))" +
                " WHERE dagbk.type_dgbk NOT IN ('W','T','R','V','M','K','B','G')" +
                " GROUP BY dagbk.afk," +
                " dagbk.dagbknr," +
                " dagbk.type_dgbk," +
                " dagbk.oms25_0" +
                " ORDER BY dagbk.dagbknr";


            string sqldb = "SELECT [dbserveraddress]," +
                "[dbservername]," +
                "convert(varchar(100),DecryptByPassPhrase('keybldigiitdept',[sqluser])) as sqluser," +
                "convert(varchar(100),DecryptByPassPhrase('keybldigiitdept',[sqlpasscode])) as sqlpasscode" +
                " FROM subsidiarynetworkinfo" +
                " WHERE realdbname='" + companyCode + "'";

            string connectionString = config.GetConnectionString("defaultdatabase");

            _logger.LogInformation($"Connection string: {connectionString}");
            _logger.LogInformation("C# HTTP trigger function processed a request.");
            DataTable dataTable = new DataTable();

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand command = new SqlCommand(sqldb, connection))
                {
                    connection.Open();
                    SqlDataReader reader = command.ExecuteReader();
                    dataTable.Load(reader);
                    reader.Close();
                }
            }

            DataTable dtexact= new DataTable();

            string conn = "Server=" + dataTable.Rows[0][0] + ";Database=" + companyCode + ";User Id=" + dataTable.Rows[0][2] + ";Password=" + dataTable.Rows[0][3];

            using (SqlConnection connectionExact = new SqlConnection(conn))
            {
                using (SqlCommand commandExact = new SqlCommand(sql, connectionExact))
                {
                    connectionExact.Open();
                    SqlDataReader reader = commandExact.ExecuteReader();
                    dtexact.Load(reader);
                    reader.Close();
                }
            }

            var response = req.CreateResponse(HttpStatusCode.OK);
            List<object> rows = new List<object>();
            foreach (DataRow row in dtexact.Rows)
            {
                var rowData = new Dictionary<string, object>();
                foreach (DataColumn column in dtexact.Columns)
                {
                    rowData[column.ColumnName] = row[column];
                }
                rows.Add(rowData);
            }
            await response.WriteAsJsonAsync(rows);
            return response;
        }

        public class GetReceiptType
        {
            public string companycode { get; set; }
            public string creditorno { get; set; }
            public string pono { get; set; }
            public string invtobereceivedfrom { get; set; }
            public string invtobereceivedto { get; set; }
            public string yourref { get; set; }
            public string ourref { get; set; }
            public string currencycode { get; set; }

        }

        [Function("GetReceiptInfoByItems")]
        public async Task<HttpResponseData> GetReceiptInfoByItems([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "GetReceiptInfoByItems")] HttpRequestData req)
        {
            // requestbodyの内容を読み取る
            string requestBody = await req.ReadAsStringAsync();
            dynamic requestData = JsonConvert.DeserializeObject<GetReceiptType>(requestBody);

            string companyCode = requestData.companycode;
            string creditorNo = requestData.creditorno;
            string poNo = requestData.pono;
            string invtobereceivedFrom = requestData.invtobereceivedfrom;
            string invtobereceivedTo = requestData.invtobereceivedto;
            string yourRef = requestData.yourref;
            string ourRef = requestData.ourref;
            string currencyCode = requestData.currencycode;

            // SQLクエリのwhereの構成
            string creditorNowhereClause = "";
            string creditorNowhereClause2 = "";
            string creditorNowhereClause3 = "";
            string creditorNowhereClause4 = "";
            string creditorNowhereClause5 = "";
            string poNowhereClause = "";
            string poNowhereClause2 = "";
            string poNowhereClause3 = "";
            string poNowhereClause4 = "";
            string poNowhereClause5 = "";
            string invtobereceivedwhereClause = "";
            string invtobereceivedwhereClause2 = "";
            string yourRefwhereClause = "";
            string yourRefwhereClause2 = "";
            string ourRefwhereClause = "";
            string ourRefwhereClause2 = "";
            string currencyCodewhereClause = "";

            if (!string.IsNullOrEmpty(creditorNo))
            {
                creditorNowhereClause += " AND o.crdnr =@param1 ";
                creditorNowhereClause2 += " AND creditornumber = @param1";
                creditorNowhereClause3 += " AND r.crdnr = @param1";
                creditorNowhereClause4 += " AND inv.crdnr = @param1";
                creditorNowhereClause5 += " AND FulFillPO.crdnr = @param1";
            }

            if (!string.IsNullOrEmpty(poNo))
            {
                poNowhereClause += " AND o.bkstnr_sub =@param2";
                poNowhereClause2 += " AND ordernumber =@param2";
                poNowhereClause3 += " AND r.bkstnr_sub =@param2";
                poNowhereClause4 += " AND inv.bkstnr_sub =@param2";
                poNowhereClause5 += " AND FulFillPO.bkstnr_sub =@param2";

            }

            if (!string.IsNullOrEmpty(yourRef))
            {
                yourRefwhereClause += " AND r.docnumber LIKE @param3"+ "%";
                yourRefwhereClause2 += " AND FulFillPO.docnumber LIKE @param3"+ "%";
                //    " AND FulFillPO.docnumber LIKE '" + yourRef + "%'";
            }

            if (!string.IsNullOrEmpty(ourRef))
            {
                ourRefwhereClause += " AND(r.faktuurnr LIKE @param4"+"% OR r.faktuurnr = @param4)";
                ourRefwhereClause2 += " AND(FulFillPO.faktuurnr LIKE @param4"+ "% OR FulFillPO.faktuurnr = @param4)";
                //AND(FulFillPO.faktuurnr LIKE '" + ourRef + "%' OR FulFillPO.faktuurnr = '" + ourRef + "')"
            }

            if (!string.IsNullOrEmpty(currencyCode))
            {
                currencyCodewhereClause += " AND (CASE WHEN(FulFillPO.bkstnr_sub IS NULL OR OnePO.currencycode IS NULL) THEN oneReceipt.valcode ELSE OnePO.currencycode END) = @param5";
            }

            if (!string.IsNullOrEmpty(invtobereceivedFrom)  && !string.IsNullOrEmpty(invtobereceivedTo))
            {
                invtobereceivedwhereClause += " AND (r.docdate BETWEEN @param6 and @param7)";
                invtobereceivedwhereClause2 += " AND (FulFillPO.docdate BETWEEN @param6 and @param7)";
            }
            if (string.IsNullOrEmpty(invtobereceivedFrom) && !string.IsNullOrEmpty(invtobereceivedTo))
            {
                invtobereceivedwhereClause += " AND r.docdate = @param7";
                invtobereceivedwhereClause2 += " AND FulFillPO.docdate = @param7";
            }
            if (!string.IsNullOrEmpty(invtobereceivedFrom) && string.IsNullOrEmpty(invtobereceivedTo))
            {
                invtobereceivedwhereClause += " AND r.docdate = @param6 ";
                invtobereceivedwhereClause2 += " AND FulFillPO.docdate = @param6";
            }

            //JSONファイル読み取る
            var config = new ConfigurationBuilder()
            .SetBasePath(Environment.CurrentDirectory)
            .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();

            string sql = "SELECT blockedterm,docdate as [date],afldat as [fulfilmentdate],po_number as [pono]," +
                " po_description as [description]," +
                " qtyordered as [qty]," +
                " unitsordered as [units],unitfactor as [factor]," +
                " qtyreceived as [received]," +
                " qtyinv as [invoicedperitempo]," +
                " unitsfulfilled as [unitsreceived]," +
                " ourref," +
                " yourref," +
                " entrynumber," +
                " itemcode as [item]," +
                " itemdescr as [itemdescription]," +
                " supplieritemcode as [supplyitemcode]," +
                " serialnumber as [serialbatchno]," +
                " receiptid ," +
                " projectcode," +
                " currencycode," +
                " rate," +
                " itemprice," +
                " amtordered as [outstanding]," +
                " amtorderedvat as [vat]," +
                " vatcode," +
                " amtorderedincvat as [outstamtinclvat]," +
                " reknr as [glno]" +
                " FROM" +
                " (SELECT Max(Terms.blocked) AS BlockedTerm," +
                " Max(FulFillPO.docdate) AS docdate," +
                " Max(FulFillPO.afldat) AS afldat," +
                " Max(totalordered) AS QtyOrdered," +
                " Max(unitordered) AS UnitsOrdered," +
                " Max(Isnull(OneReceipt.unitfactor, Isnull(OnePO.unitfactor, (CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END)))) AS UnitFactor," +
                " Max(CASE WHEN FulFillPO.transsubtype = 'J' THEN qtyreturned ELSE qtyfulfilled END) AS QtyReceived," +
                " Max(CASE WHEN FulFillPO.transsubtype = 'J' THEN Isnull(InvPO.qtyinvoicedreturnsunits / Isnull(OneReceipt.unitfactor, Isnull(OnePO.unitfactor, (CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END))), 0) ELSE Isnull(InvPO.qtyinvoicedunits / Isnull(OneReceipt.unitfactor, Isnull(OnePO.unitfactor, (CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END))), 0) END) AS QtyInv," +
                " Max(-FulFillPO.aantal) AS UnitsFulFilled," +
                " Max(FulFillPO.bkstnr_sub) AS PO_Number," +
                " Max(FulFillPO.faktuurnr) AS OurRef," +
                " Max(FulFillPO.docnumber) AS YourRef," +
                " Max(Isnull(FulFillPO.oms25, '')) AS PO_Description," +
                " Max(FulFillPO.artcode) AS ItemCode," +
                " Max(i.description) AS ItemDescr," +
                " Max(al.itemcodeaccount) AS SupplierItemCode," +
                " Max(FulFillPO.bkstnr) AS EntryNumber," +
                " Max(FulFillPO.facode) AS SerialNumber," +
                " FulFillPO.id AS ReceiptID," +
                " Max(CASE WHEN(FulFillPO.bkstnr_sub IS NULL OR OnePO.currencycode IS NULL) THEN oneReceipt.valcode ELSE OnePO.currencycode END) AS CurrencyCode," +
                " Max(Isnull(OnePO.rate, oneReceipt.rate)) AS Rate," +
                " Max(FulFillPO.project) AS ProjectCode," +
                " Max(Isnull(oneReceipt.itemprice, Isnull(OnePO.itemprice, (CASE WHEN OnePO.vattype = 'I' THEN((FulFillPO.bdr_hfl * (1 + OnePO.vatperc / 100)) / oneReceipt.rate) / CASE WHEN(FulFillPO.aantal / Isnull(OneReceipt.unitfactor, Isnull(OnePO.unitfactor, (CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END)))) <> 0 THEN(FulFillPO.aantal / Isnull(OneReceipt.unitfactor, Isnull(OnePO.unitfactor, (CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END)))) ELSE 1 END ELSE(FulFillPO.bdr_hfl / oneReceipt.rate) / CASE WHEN(FulFillPO.aantal / Isnull(OneReceipt.unitfactor, Isnull(OnePO.unitfactor, (CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END))) ) <> 0 THEN(FulFillPO.aantal / Isnull(OneReceipt.unitfactor, Isnull(OnePO.unitfactor, (CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END))) ) ELSE 1 END END )))) AS ItemPrice," +
                " (Max(-FulFillPO.aantal / Isnull(OneReceipt.unitfactor, Isnull(OnePO.unitfactor, (CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END)))) * Max((Isnull(oneReceipt.amtreceived, Isnull(onePO.amtordered / CASE WHEN onePO.qtyordered<> 0 THEN onePO.qtyordered ELSE 1 END, (-FulFillPO.bdr_hfl / oneReceipt.rate) / (-FulFillPO.aantal / Isnull(OneReceipt.unitfactor, Isnull(OnePO.unitfactor, (CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END)))))))) ) AmtOrdered," +
                " (Max(-FulFillPO.aantal / Isnull(OneReceipt.unitfactor, Isnull(OnePO.unitfactor, (CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END)))) * Max((Isnull(oneReceipt.amtvat, Isnull(onePO.amtorderedvat / CASE WHEN onePO.qtyordered<> 0 THEN onePO.qtyordered ELSE 1 END, ((-FulFillPO.bdr_hfl * Isnull(OnePO.vatperc, btwtrs.btwper) / 100) / oneReceipt.rate) / CASE WHEN(-FulFillPO.aantal / Isnull(OneReceipt.unitfactor, Isnull(OnePO.unitfactor, (CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END)))) <> 0 THEN(-FulFillPO.aantal / Isnull(OneReceipt.unitfactor, Isnull(OnePO.unitfactor, (CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END)))) ELSE 1 END)) )) ) AmtOrderedVAT," +
                " Max(Isnull(OneReceipt.vatcode, Isnull(OnePO.vatcode, al.purchasevatcode))) AS VATCode," +
                " (Max(-FulFillPO.aantal / Isnull(OneReceipt.unitfactor, Isnull(OnePO.unitfactor, (CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END)))) * Max((Isnull(oneReceipt.amtrecincvat, Isnull((onePO.amtordered + onePO.amtorderedvat) / CASE WHEN onePO.qtyordered<> 0 THEN onePO.qtyordered ELSE 1 END, ((-FulFillPO.bdr_hfl * (1 + Isnull(OnePO.vatperc, btwtrs.btwper) / 100)) / oneReceipt.rate) / (-FulFillPO.aantal / Isnull(OneReceipt.unitfactor, Isnull(OnePO.unitfactor, (CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END)))))))) ) AS AmtOrderedIncVAT," +
                " Max(FulFillPO.reknr) reknr" +
                " FROM gbkmut FulFillPO with(nolock)" +
                " LEFT JOIN(" +
                " SELECT o.entryguid," +
                " o.transsubtype," +
                " o.bkstnr_sub AS PO_Number," +
                " o.artcode AS ItemCode," +
                " (CASE WHEN org.unitfactor<> 0 THEN org.unitfactor ELSE (CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END ) END ) AS UnitFactor," +
                " -o.aantal / (CASE WHEN org.unitfactor<> 0 THEN org.unitfactor ELSE (CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END ) END ) AS QtyOrdered," +
                " -o.aantal AS QtyOrderedUnits," +
                " -o.bdr_val AS AmtOrdered," +
                " o.btw_code AS VATCode," +
                " o.btw_bdr_3 AS AmtOrderedVAT," +
                " o.koers AS Rate," +
                " o.valcode AS CurrencyCode," +
                " Isnull(org.prijs_n, 0) AS ItemPrice," +
                " btwtrs.btwper AS VatPerc," +
                " btwtrs.exclus AS VatType" +
                " FROM gbkmut o with(nolock)" +
                " INNER JOIN orsrg org with(nolock)" +
                " ON org.ordernr = o.bkstnr_sub" +
                " AND org.regel = o.regel" +
                " INNER JOIN btwtrs with(nolock)" +
                " ON o.btw_code = btwtrs.btwtrans" +
                " INNER JOIN itemaccounts al with(nolock)" +
                " ON al.itemcode = o.artcode" +
                " AND al.crdnr = o.crdnr" +
                " WHERE o.transtype = 'B'" +
                " AND o.transsubtype IN( 'A', 'J' )" +
                " AND o.freefield1 IN( 'B', 'D', 'L' )" +
                " AND o.bud_vers = 'MRP'" +
                " AND o.checked = 1" +
                " AND o.reviewed = 1" +
                " AND o.reknr IN( '   300002' )" + creditorNowhereClause +poNowhereClause +
                //"AND o.crdnr = '" + creditorNo + "'--creditorno / where" +
                //"AND o.bkstnr_sub = '" + poNo + "'--pono / where" +
                " ) AS OnePO" +
                " ON FulFillPO.bkstnr_sub = OnePO.po_number" +
                " AND FulFillPO.bkstnr_sub IS NOT NULL" +
                " AND OnePO.po_number IS NOT NULL" +
                " AND FulFillPO.artcode = OnePO.itemcode" +
                " AND FulFillPO.artcode IS NOT NULL" +
                " AND OnePO.itemcode IS NOT NULL" +
                " AND FulFillPO.transsubtype = OnePO.transsubtype" +
                " AND FulFillPO.transsubtype IS NOT NULL" +
                " AND OnePO.transsubtype IS NOT NULL" +
                " AND(FulFillPO.linkedline = OnePO.entryguid" +
                " OR FulFillPO.linkedline IS NULL)" +
                " LEFT JOIN(SELECT ordernumber," +
                " Max(blocked) AS Blocked" +
                " FROM banktransactions with (nolock)" +
                " WHERE type = 'W'" +
                " AND status <> 'V'" + creditorNowhereClause2 +
                //"AND creditornumber = '" +creditorNo + "'--creditorno / where" +
                " AND ordernumber IS NOT NULL" +
                " AND entrynumber IS NULL" +
                " AND invoicenumber IS NULL" +
                " AND matchid IS NULL" +
                " AND batchnumber = 0" +poNowhereClause2 +
                //"AND ordernumber = '" +poNo + "'-- pono / where" +
                " GROUP BY ordernumber) AS Terms" +
                " ON Terms.ordernumber = FulFillPO.bkstnr_sub" +
                " AND Terms.ordernumber IS NOT NULL" +
                " AND FulFillPO.bkstnr_sub IS NOT NULL" +
                " INNER JOIN(SELECT r.id," +
                " (CASE WHEN org.unitfactor<> 0 THEN org.unitfactor" +
                " ELSE(CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END) END ) AS UnitFactor," +
                " o.valcode," +
                " o.koers AS rate," +
                " org.prijs_n AS ItemPrice," +
                " org.btw_code AS VATCode," +
                " (org.bdr_val - org.bdr_ev_ed_val) / org.esr_aantal AS AmtVAT," +
                " org.bdr_ev_ed_val / org.esr_aantal AS AmtReceived," +
                " org.bdr_val / org.esr_aantal AS AmtRecIncVat," +
                " r.bdr_hfl AS AmtFulfilled," +
                " (CASE WHEN r.transsubtype = 'A' THEN(-r.aantal / (CASE WHEN org.unitfactor<> 0 THEN org.unitfactor ELSE(CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END) END) ) ELSE 0 END ) AS QtyFulfilled," +
                " ( CASE WHEN r.transsubtype = 'J' THEN(-r.aantal / (CASE WHEN org.unitfactor<> 0 THEN org.unitfactor ELSE(CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END) END) ) ELSE 0 END ) AS QtyReturned" +
                " FROM gbkmut r WITH(nolock)" +
                " LEFT JOIN orsrg org WITH(nolock)" +
                " ON r.linkedline = org.sysguid" +
                " INNER JOIN orkrg o WITH(nolock)" +
                " ON r.bkstnr_sub = o.ordernr" +
                " INNER JOIN itemaccounts al WITH(nolock)" +
                " ON al.itemcode = r.artcode" +
                " AND al.crdnr = r.crdnr" +
                " WHERE r.transtype IN( 'N', 'X' )" +
                " AND r.transsubtype IN( 'A', 'J' )" +
                " AND r.reknr IN( '   300002' )" + creditorNowhereClause3 +invtobereceivedwhereClause +
                //"AND r.crdnr = '" +creditorNo+ "'--creditorno / where" +
                //"AND r.docdate >= { d '" + invtobereceivedFrom + "'}" +
                //" --invtobereceivedfrom / where" +
                //"AND r.docdate <= { d '" + invtobereceivedTo + "'}" +
                //"--invtobereceivedto / where"
                poNowhereClause3 +yourRefwhereClause + ourRefwhereClause +
                //"AND r.bkstnr_sub = '" +poNo + "'-- pono / where" +
                //"--AND r.docnumber LIKE '" +yourRef+ "%'--yourref / where" +
                //"AND(r.faktuurnr LIKE '" +ourRef + "%' OR r.faktuurnr = '" + ourRef + "')-- ourref / where" +
                " ) AS OneReceipt" +
                " ON OneReceipt.id = FulFillPO.id" +
                " LEFT JOIN (SELECT inv.bkstnr_sub," +
                " inv.crdnr," +
                " inv.artcode," +
                " Sum(CASE WHEN ((inv.aantal >= 0 AND inv.oorsprong<> 'R' )" +
                " OR(inv.aantal < 0 AND inv.oorsprong = 'R') ) THEN inv.aantal" +
                " ELSE 0 END) AS QtyInvoicedUnits," +
                " Sum(CASE WHEN((inv.aantal < 0 AND inv.oorsprong<> 'R')" +
                " OR(inv.aantal >= 0" +
                " AND inv.oorsprong = 'R') ) THEN inv.aantal" +
                " ELSE 0 END) AS QtyInvoicedReturnsUnits," +
                " Sum(inv.bdr_val) AS AmtInvoiced," +
                " Sum(inv.btw_bdr_3 / inv.koers) AS AmtInvoicedVAT" +
                " FROM gbkmut inv WITH(nolock)" +
                " WHERE inv.transtype IN( 'N', 'X' )" +
                " AND inv.transsubtype IN( 'T', 'Q' )" +
                " AND inv.reknr IN( '   300002' )" + creditorNowhereClause4 +poNowhereClause4 +
                //"AND inv.crdnr = '" +creditorNo+ "'--creditorno / where" +
                //"AND inv.bkstnr_sub = '" +poNo + "'--pono / where" +
                " GROUP BY inv.bkstnr_sub," +
                " inv.crdnr," +
                " inv.artcode" +
                " ) AS InvPO" +
                " ON InvPO.bkstnr_sub = FulFillPO.bkstnr_sub" +
                " AND InvPO.crdnr = FulFillPO.crdnr" +
                " AND InvPO.crdnr IS NOT NULL" +
                " AND FulFillPO.crdnr IS NOT NULL" +
                " AND InvPO.artcode = FulFillPO.artcode" +
                " AND InvPO.artcode IS NOT NULL" +
                " AND FulFillPO.artcode IS NOT NULL" +
                " INNER JOIN(SELECT r.bkstnr_sub," +
                " r.artcode," +
                " Sum(CASE WHEN r.transtype IN ( 'N', 'X' ) AND r.transsubtype = 'A' THEN(-r.aantal /" +
                " (CASE WHEN org.unitfactor<> 0 THEN org.unitfactor ELSE(CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END) END) ) ELSE 0 END) AS TotalReceived," +
                " Sum(CASE WHEN r.transtype IN('N', 'X') AND r.transsubtype = 'J' THEN(-r.aantal / (CASE WHEN org.unitfactor<> 0 THEN org.unitfactor ELSE(CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END) END) ) ELSE 0 END) AS TotalReturned," +
                " Sum(CASE WHEN r.transtype IN('N', 'X') AND r.transsubtype = 'A' THEN - r.aantal ELSE 0 END) AS TotalReceivedUnits," +
                " Sum(CASE WHEN r.transtype IN('N', 'X') AND r.transsubtype = 'J' THEN - r.aantal ELSE 0 END) AS TotalReturnedUnits," +
                " Sum(CASE WHEN r.transtype = 'B' THEN(-r.aantal / (CASE WHEN org.unitfactor<> 0 THEN org.unitfactor ELSE(CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END) END) ) ELSE 0 END) AS TotalOrdered," +
                " Sum(CASE WHEN r.transtype = 'B' THEN - r.aantal ELSE 0 END) AS UnitOrdered" +
                " FROM gbkmut r WITH(nolock)" +
                " LEFT JOIN orsrg org WITH(nolock)" +
                " ON r.bkstnr_sub = org.ordernr" +
                " AND(r.entryguid = org.sysguid" +
                " OR r.linkedline = org.sysguid)" +
                " INNER JOIN itemaccounts al WITH(nolock)" +
                " ON al.itemcode = r.artcode" +
                " AND al.crdnr = r.crdnr" +
                " WHERE r.transtype IN( 'N', 'B', 'X' )" +
                " AND r.transsubtype IN( 'A', 'J' )" +
                " AND r.reknr IN( '   300002' )" + creditorNowhereClause3 +
                //"AND r.crdnr = '" +creditorNo + "'--creditorno / where" +
                " GROUP BY r.bkstnr_sub," +
                " r.artcode) AS TotalReceipt" +
                " ON TotalReceipt.bkstnr_sub = FulFillPO.bkstnr_sub" +
                " AND TotalReceipt.bkstnr_sub IS NOT NULL" +
                " AND FulFillPO.bkstnr_sub IS NOT NULL" +
                " AND TotalReceipt.artcode = FulFillPO.artcode" +
                " AND TotalReceipt.artcode IS NOT NULL" +
                " AND FulFillPO.artcode IS NOT NULL" +
                " INNER JOIN items i WITH(nolock)" +
                " ON i.itemcode = FulFillPO.artcode" +
                " INNER JOIN itemaccounts al WITH(nolock)" +
                " ON al.itemcode = FulFillPO.artcode" +
                " AND al.crdnr = FulFillPO.crdnr" +
                " INNER JOIN btwtrs WITH(nolock)" +
                " ON btwtrs.btwtrans = al.purchasevatcode" +
                " WHERE FulFillPO.transtype IN( 'N', 'X' )" +
                " AND FulFillPO.transsubtype IN( 'A', 'J' )" + creditorNowhereClause5 +
                //"AND FulFillPO.crdnr = '" + creditorNo + "'--creditorno / where" +
                " AND FulFillPO.reknr IN( '   300002' )" + invtobereceivedwhereClause2 +
                //"AND FulFillPO.docdate >= { d '" + invtobereceivedFrom + "'}" +
                //"            --invtobereceivedfrom / where" +
                //"AND FulFillPO.docdate <= { d '" + invtobereceivedTo + "'}" +
                //"            --invtobereceivedto / where" +
                poNowhereClause5 +yourRefwhereClause2 +ourRefwhereClause2 +currencyCodewhereClause +
                //"                AND FulFillPO.bkstnr_sub = '" + poNo + "'-- pono / where" +
                //"               --AND FulFillPO.docnumber LIKE '" +yourRef + "%'-- yourref / where" +
                //"AND(FulFillPO.faktuurnr LIKE '" + ourRef + "%' OR FulFillPO.faktuurnr = '" + ourRef + "')-- ourref / where" +
                //"AND(CASE WHEN(FulFillPO.bkstnr_sub IS NULL OR OnePO.currencycode IS NULL) THEN oneReceipt.valcode ELSE OnePO.currencycode END) = '" +currencyCode + "'--currencycode / where" +
                " AND((FulFillPO.transsubtype = 'A'" +
                " AND FulFillPO.bkstnr_sub IS NOT NULL" +
                " AND Round(Isnull(InvPO.qtyinvoicedunits, 0), 3) < Round(TotalReceipt.totalreceivedunits, 3))" +
                " OR(FulFillPO.transsubtype = 'J'" +
                " AND FulFillPO.bkstnr_sub IS NOT NULL" +
                " AND Abs(Round(Isnull(InvPO.qtyinvoicedreturnsunits, 0), 3)) < Abs(Round(TotalReceipt.totalreturnedunits, 3))" +
                " ) )" +
                " GROUP BY FulFillPO.id" +
                " HAVING 1 = 1) ITR" +
                " ORDER BY ITR.ItemCode";

            string sqldb = "SELECT [dbserveraddress]," +
                "[dbservername]," +
                "convert(varchar(100),DecryptByPassPhrase('keybldigiitdept',[sqluser])) as sqluser," +
                "convert(varchar(100),DecryptByPassPhrase('keybldigiitdept',[sqlpasscode])) as sqlpasscode" +
                " FROM subsidiarynetworkinfo" +
                " WHERE realdbname= @param ";

            string connectionString = config.GetConnectionString("defaultdatabase");

            _logger.LogInformation($"Connection string: {connectionString}");
            _logger.LogInformation("C# HTTP trigger function processed a request.");
            DataTable dataTable = new DataTable();


            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand command = new SqlCommand(sqldb, connection))
                {
                    connection.Open();
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@param", companyCode);
                    SqlDataReader reader = command.ExecuteReader();
                    dataTable.Load(reader);
                    reader.Close();
                }
            }

            DataTable dtexact = new DataTable();

            string conn = "Server=" + dataTable.Rows[0][0] + ";Database=" + companyCode + ";User Id=" + dataTable.Rows[0][2] + ";Password=" + dataTable.Rows[0][3];

            using (SqlConnection connectionExact = new SqlConnection(conn))
            {
                using (SqlCommand commandExact = new SqlCommand(sql, connectionExact))
                {
                    connectionExact.Open();
                    commandExact.Parameters.Clear();

                    if (!string.IsNullOrEmpty(creditorNo))
                    {
                        commandExact.Parameters.AddWithValue("@param1", creditorNo);
                    }
                    else
                    {
                        commandExact.Parameters.AddWithValue("@param1", DBNull.Value);
                    }

                    if (!string.IsNullOrEmpty(poNo))
                    {
                        commandExact.Parameters.AddWithValue("@param2", poNo);
                    }
                    else
                    {
                        commandExact.Parameters.AddWithValue("@param2", DBNull.Value);
                    }

                    if (!string.IsNullOrEmpty(yourRef))
                    {
                        commandExact.Parameters.AddWithValue("@param3", yourRef);
                    }
                    else
                    {
                        commandExact.Parameters.AddWithValue("@param3", DBNull.Value);
                    }

                    if (!string.IsNullOrEmpty(ourRef))
                    {
                        commandExact.Parameters.AddWithValue("@param4", ourRef);
                    }
                    else
                    {
                        commandExact.Parameters.AddWithValue("@param4", DBNull.Value);
                    }

                    if (!string.IsNullOrEmpty(currencyCode))
                    {
                        commandExact.Parameters.AddWithValue("@param5", currencyCode);
                    }
                    else
                    {
                        commandExact.Parameters.AddWithValue("@param5", DBNull.Value);
                    }

                    if (!string.IsNullOrEmpty(invtobereceivedFrom))
                    {
                        commandExact.Parameters.AddWithValue("@param6", invtobereceivedFrom);
                    }
                    else
                    {
                        commandExact.Parameters.AddWithValue("@param6", DBNull.Value);
                    }

                    if (!string.IsNullOrEmpty(invtobereceivedTo))
                    {
                        commandExact.Parameters.AddWithValue("@param7", invtobereceivedTo);
                    }
                    else
                    {
                        commandExact.Parameters.AddWithValue("@param7", DBNull.Value);
                    }

                    SqlDataReader reader = commandExact.ExecuteReader();
                    dtexact.Load(reader);
                    reader.Close();
                }
            }

            var response = req.CreateResponse(HttpStatusCode.OK);
            List<object> rows = new List<object>();
            foreach (DataRow row in dtexact.Rows)
            {
                var rowData = new Dictionary<string, object>();
                foreach (DataColumn column in dtexact.Columns)
                {
                    rowData[column.ColumnName] = row[column];
                }
                rows.Add(rowData);
            }
            await response.WriteAsJsonAsync(rows);
            return response;
        }

        public class GetReceiptGroupType
        {
            public string companycode { get; set; }
            public string creditorno { get; set; }
            public string pono { get; set; }
            public string invtobereceivedfrom { get; set; }
            public string invtobereceivedto { get; set; }
        }

        [Function("GetReceiptInfoByItemsGroup")]
        public async Task<HttpResponseData> GetReceiptInfoByItemsGroup([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "GetReceiptInfoByItemsGroup")] HttpRequestData req)
        {
            // requestbodyの内容を読み取る
            string requestBody = await req.ReadAsStringAsync();
            dynamic requestData = JsonConvert.DeserializeObject<GetReceiptGroupType>(requestBody);
            string companyCode = requestData.companycode;
            string creditorNo = requestData.creditorno;
            string poNo = requestData.pono;
            string invtobereceivedFrom = requestData.invtobereceivedfrom;
            string invtobereceivedTo = requestData.invtobereceivedto;

            // SQLクエリのwhereの構成
            string creditorNowhereClause = "";
            string creditorNowhereClause2 = "";
            string creditorNowhereClause3 = "";
            string creditorNowhereClause4 = "";
            string creditorNowhereClause5 = "";
            string poNowhereClause = "";          
            string invtobereceivedwhereClause = "";

            if (!string.IsNullOrEmpty(creditorNo))
            {
                creditorNowhereClause += " AND orkrg.crdnr =@param1 ";
                creditorNowhereClause2 += " AND r.crdnr = @param1";
                creditorNowhereClause3 += " AND o.crdnr = @param1";
                creditorNowhereClause4 += " AND creditornumber = @param1";
                creditorNowhereClause5 += " AND inv.crdnr = @param1";
            }

            if (!string.IsNullOrEmpty(poNo))
            {
                poNowhereClause += " where po_number = @param2";

            }

            if (!string.IsNullOrEmpty(invtobereceivedFrom) && !string.IsNullOrEmpty(invtobereceivedTo))
            {
                invtobereceivedwhereClause += " AND (r.docdate BETWEEN @param3 and @param4)";
            }
            if (string.IsNullOrEmpty(invtobereceivedFrom) && !string.IsNullOrEmpty(invtobereceivedTo))
            {
                invtobereceivedwhereClause += " AND r.docdate = @param4";
            }
            if (!string.IsNullOrEmpty(invtobereceivedFrom) && string.IsNullOrEmpty(invtobereceivedTo))
            {
                invtobereceivedwhereClause += " AND r.docdate = @param3";
            }

            //JSONファイル読み取る
            var config = new ConfigurationBuilder()
            .SetBasePath(Environment.CurrentDirectory)
            .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();

            string sql = "SELECT blockedterm," +
                "       CONVERT(VARCHAR(10),docdate,120) as docdate ," +
                "       CONVERT(VARCHAR(10),afldat,120) as afldat," +
                "       po_number," +
                "       po_description," +
                "       docnumber," +
                "       qtyordered," +
                "       unitsordered," +
                "       unitfactor," +
                "       qtyrec," +
                "       qtyinv," +
                "       qtyreceived," +
                "       unitsfulfilled," +
                "       itemcode," +
                "       itemdescr," +
                "       supplieritemcode," +
                "       itemprice," +
                "       currencycode," +
                "       rate," +
                "       projectcode," +
                "       amtordered," +
                "       vatcode," +
                "       amtorderedvat," +
                "       amtorderedincvat," +
                "       transsubtype," +
                "       reknr" +
                " FROM(SELECT Max(Terms.blocked) AS BlockedTerm," +
                " Max(FulFillPO.docdate) AS docDate," +
                " Max(FulFillPO.afldat) AS afldat," +
                " Max(FulFillPO.fulfillnumber) AS DocNumber," +
                " Max(Isnull(OnePO.qtyordered, 0)) AS QtyOrdered," +
                " Max(Isnull(OnePO.qtyorderedunits, 0)) AS UnitsOrdered," +
                " Max(Isnull(OnePO.unitfactor, FulFillPO.unitfactor)) AS UnitFactor," +
                " Max(FulFillPO.qtyreceived) AS QtyRec," +
                " Max(CASE WHEN FulFillPO.transsubtype = 'J' THEN Isnull(InvPO.qtyinvoicedreturnsunits / Isnull(OnePO.unitfactor, FulFillPO.unitfactor), 0) ELSE Isnull(InvPO.qtyinvoicedunits / Isnull(OnePO.unitfactor, FulFillPO.unitfactor), 0) END) AS QtyInv," +
                " Max(FulFillPO.qtyreceived - CASE WHEN FulFillPO.transsubtype = 'J' THEN Isnull(InvPO.qtyinvoicedreturnsunits / Isnull(OnePO.unitfactor, FulFillPO.unitfactor), 0) ELSE Isnull(InvPO.qtyinvoicedunits / Isnull(OnePO.unitfactor, FulFillPO.unitfactor), 0) END) AS QtyReceived," +
                " Max(FulFillPO.qtyreceivedunits - CASE WHEN FulFillPO.transsubtype = 'J' THEN Isnull(InvPO.qtyinvoicedreturnsunits, 0) ELSE Isnull(InvPO.qtyinvoicedunits, 0) END) AS UnitsFulFilled," +
                " FulFillPO.po_number                                     AS PO_Number," +
                " Max(Isnull(FulFillPO.po_description, ''))               AS PO_Description," +
                " FulFillPO.itemcode                                      AS ItemCode," +
                " Max(FulFillPO.itemdescr)                                AS ItemDescr," +
                " Max(FulFillPO.itemcodeaccount)                          AS SupplierItemCode," +
                " Max(FulFillPO.projectcode)                              AS ProjectCode," +
                " Max(Isnull(FulFillPO.itemprice, Isnull(OnePO.itemprice, (CASE WHEN FulFillPO.vattype = 'I' THEN FulFillPO.amtfulfilled * (1 + FulFillPO.vatperc / 100) ELSE FulFillPO.amtfulfilled END / CASE WHEN FulFillPO.rate<> 0 THEN FulFillPO.rate ELSE 1 END) / CASE WHEN FulFillPO.qtyreceived<> 0 THEN FulFillPO.qtyreceived ELSE 1 END))) AS ItemPrice," +
                " Max(Isnull(OnePO.currencycode, FulFillPO.currencycode)) AS CurrencyCode," +
                " Max(Isnull(OnePO.rate, FulFillPO.rate))                 AS Rate," +
                " (CASE WHEN FulFillPO.transsubtype = 'A' THEN 'Receipt' ELSE 'Return' END ) AS Transsubtype," +
                " Max(CASE WHEN FulFillPO.qtyreceived - Isnull(CASE WHEN FulFillPO.transsubtype = 'J' THEN InvPO.qtyinvoicedreturnsunits ELSE InvPO.qtyinvoicedunits END / Isnull(OnePO.unitfactor, FulFillPO.unitfactor), 0) = 0 THEN Isnull(OnePO.amtordered, FulFillPO.amtfulfilled * FulFillPO.rate) - Isnull(InvPO.amtinvoiced, 0) ELSE(FulFillPO.qtyreceived - Isnull(CASE WHEN FulFillPO.transsubtype = 'J' THEN InvPO.qtyinvoicedreturnsunits ELSE InvPO.qtyinvoicedunits END / Isnull(OnePO.unitfactor, FulFillPO.unitfactor), 0)) * Isnull(FulFillPO.amtorderedexcvatperqty, Isnull((OnePO.amtordered / CASE WHEN OnePO.qtyordered<> 0 THEN OnePO.qtyordered ELSE 1 END), (FulFillPO.amtfulfilled / CASE WHEN FulFillPO.rate<> 0 THEN FulFillPO.rate ELSE 1 END / CASE WHEN FulFillPO.qtyreceived<> 0 THEN FulFillPO.qtyreceived ELSE 1 END))) END) AS AmtOrdered," +
                " Max(CASE WHEN(FulFillPO.po_number IS NULL OR OnePO.amtordered IS NULL OR(OnePO.amtordered = 0 AND FulFillPO.amtfulfilled<> 0)) THEN Isnull(FulFillPO.vatcode, FULFILLPO.f_vatcode) ELSE OnePO.vatcode END) AS VATCode," +
                " Max(CASE WHEN FulFillPO.qtyreceived - Isnull(CASE WHEN FulFillPO.transsubtype = 'J' THEN InvPO.qtyinvoicedreturnsunits ELSE InvPO.qtyinvoicedunits END / Isnull(OnePO.unitfactor, FulFillPO.unitfactor), 0) = 0 THEN Isnull(OnePO.amtorderedvat, FulFillPO.amtfulfilledvat / CASE WHEN FulFillPO.rate<> 0 THEN FulFillPO.rate ELSE 1 END) - Isnull(InvPO.amtinvoicedvat, 0) ELSE(FulFillPO.qtyreceived - Isnull(CASE WHEN FulFillPO.transsubtype = 'J' THEN InvPO.qtyinvoicedreturnsunits ELSE InvPO.qtyinvoicedunits END / Isnull(OnePO.unitfactor, FulFillPO.unitfactor), 0)) * Isnull(FulFillPO.amtorderedvatperqty, Isnull(OnePO.amtorderedvat / CASE WHEN OnePO.qtyordered<> 0 THEN OnePO.qtyordered ELSE 1 END, FulFillPO.amtfulfilled / CASE WHEN FulFillPO.rate<> 0 THEN FulFillPO.rate ELSE 1 END * (Isnull(FulFillPO.vatperc, FulFillPO.f_vatperc) / 100) / CASE WHEN FulFillPO.qtyreceived<> 0 THEN FulFillPO.qtyreceived ELSE 1 END)) END) AS AmtOrderedVAT," +
                " Max(CASE WHEN FulFillPO.qtyreceived - Isnull(CASE WHEN FulFillPO.transsubtype = 'J' THEN InvPO.qtyinvoicedreturnsunits ELSE InvPO.qtyinvoicedunits END / Isnull(OnePO.unitfactor, FulFillPO.unitfactor), 0) = 0 THEN Isnull(OnePO.amtordered + OnePO.amtorderedvat, FulFillPO.amtfulfilled / CASE WHEN FulFillPO.rate<> 0 THEN FulFillPO.rate ELSE 1 END) - Isnull(InvPO.amtinvoiced + InvPO.amtinvoicedvat, 0) ELSE(FulFillPO.qtyreceived - Isnull(CASE WHEN FulFillPO.transsubtype = 'J' THEN InvPO.qtyinvoicedreturnsunits ELSE InvPO.qtyinvoicedunits END / Isnull(OnePO.unitfactor, FulFillPO.unitfactor), 0)) * Isnull(FulFillPO.amtorderedexcvatperqty + FulFillPO.amtorderedvatperqty, Isnull((OnePO.amtordered + OnePO.amtorderedvat) / CASE WHEN OnePO.qtyordered<> 0 THEN OnePO.qtyordered ELSE 1 END, FulFillPO.amtfulfilled / CASE WHEN FulFillPO.rate<> 0 THEN FulFillPO.rate ELSE 1 END * (1 + Isnull(FulFillPO.vatperc, FulFillPO.f_vatperc) / 100) / CASE WHEN FulFillPO.qtyreceived<> 0 THEN FulFillPO.qtyreceived ELSE 1 END)) END)  AS AmtOrderedIncVAT," +
                " Max(FulFillPO.reknr) AS reknr" +
                " FROM" +
                " (SELECT r.bkstnr_sub AS PO_Number," +
                " Max(Isnull(r.oms25, '')) AS PO_Description," +
                " Max(r.docdate) AS docdate," +
                " Max(r.afldat) AS afldat," +
                " Max(r.docnumber) AS FulFillNumber," +
                " r.artcode AS ItemCode," +
                " r.transsubtype AS Transsubtype," +
                " Max(Isnull(org.oms45, i.description)) AS ItemDescr," +
                " Max(al.itemcodeaccount) AS ItemCodeAccount," +
                " Max((CASE WHEN org.unitfactor<> 0 THEN org.unitfactor ELSE(CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END) END)) AS UnitFactor," +
                " Max(r.reknr) AS reknr," +
                " Max(r.project) AS ProjectCode," +
                " Max(r.btw_code) AS f_VATCode," +
                " Max(btw.btwper) AS f_VatPerc," +
                " Max(btw.exclus) AS f_VatType," +
                " Max(Isnull(org.btw_code, org2.btw_code)) AS VATCode," +
                " Max(btw2.btwper) AS VatPerc," +
                " Max(btw2.exclus) AS VatType," +
                " Max(o.valcode) AS CurrencyCode," +
                " Max(o.koers) AS Rate," +
                " Sum(-r.aantal / (CASE WHEN Isnull(org.unitfactor, 0) <> 0 THEN org.unitfactor ELSE(CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END) END )) AS QtyReceived," +
                " Sum(-r.aantal) AS QtyReceivedUnits," +
                " Sum(-r.bdr_hfl) AS AmtFulfilled," +
                " Sum(-r.btw_bdr_3) AS AmtFulfilledVAT," +
                " Sum(r.bdr_hfl * o.koers) / Sum(r.aantal / (CASE WHEN org.unitfactor<> 0 THEN org.unitfactor ELSE(CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END) END)) AS f_ItemPrice," +
                " Sum(Isnull(org.prijs_n, org2.prijs_n) * -r.aantal) / Sum(-r.aantal) AS ItemPrice," +
                " Sum(Isnull((org.bdr_ev_ed_val) / org.esr_aantal, (org2.bdr_ev_ed_val) / org2.esr_aantal) * -r.aantal / (CASE WHEN Isnull(org.unitfactor, Isnull(org2.unitfactor, 0)) <> 0 THEN Isnull(org.unitfactor, org2.unitfactor)" +
                " ELSE(CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END) END )) / Sum(-r.aantal / (CASE WHEN Isnull(org.unitfactor, Isnull(org2.unitfactor, 0)) <> 0" +
                " THEN Isnull(org.unitfactor, org2.unitfactor) ELSE(CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END) END)) AS AmtOrderedExcVATPerQty," +
                " Sum(Isnull((org.bdr_val - org.bdr_ev_ed_val) / org.esr_aantal, (org2.bdr_val - org2.bdr_ev_ed_val) / org2.esr_aantal) * -r.aantal / (CASE WHEN Isnull(org.unitfactor, Isnull(org2.unitfactor, 0)) <> 0 THEN Isnull(org.unitfactor, org2.unitfactor) ELSE(CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END) END )) / Sum(-r.aantal / (CASE WHEN Isnull(org.unitfactor, Isnull(org2.unitfactor, 0)) <> 0 THEN Isnull(org.unitfactor, org2.unitfactor) ELSE(CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END) END)) AS AmtOrderedVATPerQty" +
                " FROM gbkmut r WITH(nolock)" +
                " LEFT JOIN orsrg org" +
                " ON r.linkedline = org.sysguid" +
                " LEFT JOIN" +
                " (SELECT Max(orsrg.id) AS ID," +
                " orsrg.ordernr," +
                " artcode," +
                " (CASE WHEN Sign(esr_aantal) < 0 THEN 'J' ELSE 'A' END ) AS ttype" +
                " FROM orsrg" +
                " INNER JOIN orkrg" +
                " ON orkrg.ordernr = orsrg.ordernr" +
                " WHERE esr_aantal<> 0" + creditorNowhereClause +
                //"AND orkrg.crdnr = '740084'--creditorno / where" +
                " GROUP BY" +
                " orsrg.ordernr," +
                " artcode," +
                " Sign(esr_aantal)) oid" +
                " ON r.bkstnr_sub = oid.ordernr" +
                " AND r.bkstnr_sub IS NOT NULL" +
                " AND oid.ordernr IS NOT NULL" +
                " AND r.artcode = oid.artcode" +
                " AND r.artcode IS NOT NULL" +
                " AND oid.artcode IS NOT NULL" +
                " AND r.transsubtype = oid.ttype" +
                " INNER JOIN orsrg org2" +
                " ON org2.id = oid.id" +
                " INNER JOIN items i" +
                " ON i.itemcode = r.artcode" +
                " INNER JOIN itemaccounts al" +
                " ON al.itemcode = r.artcode" +
                " AND al.crdnr = r.crdnr" +
                " INNER JOIN btwtrs btw" +
                " ON r.btw_code = btw.btwtrans" +
                " LEFT JOIN btwtrs btw2" +
                " ON Isnull(org.btw_code, org2.btw_code) = btw2.btwtrans" +
                " INNER JOIN orkrg o" +
                " ON r.bkstnr_sub = o.ordernr" +
                " WHERE r.transtype IN( 'N', 'X' )" +
                " AND r.transsubtype IN( 'A', 'J' )" +
                " AND r.reknr IN( '   300002' )" + creditorNowhereClause2 +invtobereceivedwhereClause +
                //"AND r.crdnr = '740084'--creditorno / where" +
                //" AND r.docdate >= { d '2000-12-24'}" +
                //"            --invtobereceivedfrom / where" +
                //" AND r.docdate <= { d '2023-06-24'}" +
                //"            --invtobereceivedto / where" +
                " AND r.aantal<> 0" +
                " GROUP BY r.bkstnr_sub," +
                " r.transsubtype," +
                " r.artcode" +
                " HAVING" +
                " Sum(r.aantal) <> 0) AS FulFillPO" +
                " LEFT JOIN(" +
                " SELECT o.bkstnr_sub AS PO_Number," +
                " o.artcode AS ItemCode," +
                " Max((CASE WHEN org.unitfactor<> 0 THEN org.unitfactor ELSE(CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END ) END )) AS UnitFactor," +
                " Sum(-o.aantal / (CASE WHEN org.unitfactor<> 0 THEN org.unitfactor ELSE(CASE WHEN al.slspkgsperpurpkg<> 0 THEN al.slspkgsperpurpkg ELSE 1 END) END )) AS QtyOrdered," +
                " Sum(-o.aantal)              AS QtyOrderedUnits," +
                " Sum(-o.bdr_val)             AS AmtOrdered," +
                " Max(btwtrs.btwper)          AS VatPerc," +
                " Max(o.btw_code)             AS VATCode," +
                " Max(Isnull(org.prijs_n, 0)) AS ItemPrice," +
                " Sum(o.btw_bdr_3)            AS AmtOrderedVAT," +
                " Max(o.valcode)              AS CurrencyCode," +
                " Max(o.koers)                AS Rate" +
                " FROM gbkmut o WITH(nolock)" +
                " INNER JOIN itemaccounts al" +
                " ON al.itemcode = o.artcode" +
                " AND al.crdnr = o.crdnr" +
                " INNER JOIN btwtrs" +
                " ON o.btw_code = btwtrs.btwtrans" +
                " INNER JOIN orsrg org" +
                " ON org.ordernr = o.bkstnr_sub" +
                " AND org.regel = o.regel" +
                " WHERE o.transtype = 'B'" +
                " AND o.transsubtype IN( 'A', 'J' )" +
                " AND o.freefield1 IN( 'B', 'D', 'L' )" +
                " AND o.bud_vers = 'MRP'" +
                " AND o.checked = 1" +
                " AND o.reviewed = 1" +
                " AND o.reknr IN( '   300002' )" + creditorNowhereClause3 +
                //"AND o.crdnr = '740084'--creditorno / where" +
                " GROUP BY o.bkstnr_sub," +
                " o.artcode," +
                " al.itemcodeaccount) AS OnePO" +
                " ON FulFillPO.po_number = OnePO.po_number" +
                " AND FulFillPO.po_number IS NOT NULL" +
                " AND OnePO.po_number IS NOT NULL" +
                " AND FulFillPO.itemcode = OnePO.itemcode" +
                " AND FulFillPO.itemcode IS NOT NULL" +
                " AND OnePO.itemcode IS NOT NULL" +
                " LEFT JOIN(SELECT ordernumber," +
                " Max(blocked) AS Blocked" +
                " FROM   banktransactions" +
                " WHERE  type = 'W'" +
                " AND status <> 'V'" + creditorNowhereClause4 +
                //"AND creditornumber = '740084'--creditorno / where" +
                " AND ordernumber IS NOT NULL" +
                " AND entrynumber IS NULL" +
                " AND invoicenumber IS NULL" +
                " AND matchid IS NULL" +
                " AND batchnumber = 0" +
                " GROUP BY ordernumber) AS Terms" +
                " ON Terms.ordernumber = FulFillPO.po_number" +
                " AND Terms.ordernumber IS NOT NULL" +
                " AND FulFillPO.po_number IS NOT NULL" +
                " LEFT JOIN" +
                " (SELECT inv.bkstnr_sub," +
                " inv.artcode," +
                " Sum(inv.aantal) AS TotalInvoicedUnits," +
                " Sum(CASE WHEN ((inv.aantal >= 0 AND inv.oorsprong<> 'R' ) OR(inv.aantal < 0 AND inv.oorsprong = 'R') ) THEN inv.aantal ELSE 0 END) AS QtyInvoicedUnits," +
                " Sum(CASE WHEN((inv.aantal < 0 AND inv.oorsprong<> 'R') OR(inv.aantal >= 0 AND inv.oorsprong = 'R') ) THEN inv.aantal ELSE 0 END) AS QtyInvoicedReturnsUnits," +
                " Sum(inv.bdr_val) AS AmtInvoiced," +
                " Sum(inv.btw_bdr_3 / inv.koers) AS AmtInvoicedVAT" +
                " FROM gbkmut inv WITH(nolock)" +
                " WHERE inv.transtype IN( 'N', 'X' )" +
                " AND inv.transsubtype IN( 'T', 'Q' )" +
                " AND inv.reknr IN( '   300002' )" + creditorNowhereClause5 +
                //"AND inv.crdnr = '740084'--creditorno / where" +
                " GROUP BY inv.bkstnr_sub," +
                " inv.artcode) AS InvPO" +
                " ON InvPO.bkstnr_sub = FulFillPO.po_number" +
                " AND InvPO.artcode = FulFillPO.itemcode" +
                " AND InvPO.artcode IS NOT NULL" +
                " AND FulFillPO.itemcode IS NOT NULL" +
                " LEFT JOIN(SELECT r.bkstnr_sub AS PO_Number," +
                " r.artcode AS ItemCode," +
                " Sum(-r.aantal) AS TotalReceivedUnits" +
                " FROM gbkmut r WITH(nolock)" +
                " WHERE r.transtype IN( 'N', 'X' )" +
                " AND r.transsubtype IN( 'A', 'J' )" +
                " AND r.reknr IN( '   300002' )" + creditorNowhereClause2 +invtobereceivedwhereClause +
                //"AND r.crdnr = '740084'--creditorno / where" +
                //" AND r.docdate >= { d '2000-12-24'}" +
                //"            --invtobereceivedfrom / where" +
                //" AND r.docdate <= { d '2023-06-24'}" +
                //"            --invtobereceivedto / where" +
                " GROUP BY r.bkstnr_sub," +
                " r.artcode) AS TotalReceipts" +
                " ON FulFillPO.po_number = TotalReceipts.po_number" +
                " AND FulFillPO.po_number IS NOT NULL" +
                " AND TotalReceipts.po_number IS NOT NULL" +
                " AND FulFillPO.itemcode = TotalReceipts.itemcode" +
                " AND FulFillPO.itemcode IS NOT NULL" +
                " AND TotalReceipts.itemcode IS NOT NULL" +
                " GROUP BY FulFillPO.po_number," +
                " FulFillPO.transsubtype," +
                " FulFillPO.itemcode" +
                " HAVING((FulFillPO.transsubtype = 'A' AND Max(Round(Isnull(InvPO.qtyinvoicedunits, 0), 3)) < Sum(Round(FulFillPO.qtyreceivedunits, 3)))" +
                " OR(FulFillPO.transsubtype = 'J' AND Max(Round(Isnull(InvPO.qtyinvoicedreturnsunits, 0), 3)) > Sum(Round(FulFillPO.qtyreceivedunits, 3))" +
                " )" +
                " ) " +
                " AND(Max(Round(TotalReceipts.totalreceivedunits, 3)) <> Max(Round(Isnull(InvPO.totalinvoicedunits, 0), 3)))" +
                " AND FulFillPO.po_number IS NOT NULL" +
                " ) ITR " +poNowhereClause;
                //"po_number = '70003862'--pono / where";

            string sqldb = "SELECT [dbserveraddress]," +
                "[dbservername]," +
                "convert(varchar(100),DecryptByPassPhrase('keybldigiitdept',[sqluser])) as sqluser," +
                "convert(varchar(100),DecryptByPassPhrase('keybldigiitdept',[sqlpasscode])) as sqlpasscode" +
                " FROM subsidiarynetworkinfo" +
                " WHERE realdbname= @param ";

            string connectionString = config.GetConnectionString("defaultdatabase");

            _logger.LogInformation($"Connection string: {connectionString}");
            _logger.LogInformation("C# HTTP trigger function processed a request.");
            DataTable dataTable = new DataTable();

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand command = new SqlCommand(sqldb, connection))
                {
                    connection.Open();
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@param", companyCode);
                    SqlDataReader reader = command.ExecuteReader();
                    dataTable.Load(reader);
                    reader.Close();
                }
            }

            DataTable dtexact = new DataTable();
            string conn = "Server=" + dataTable.Rows[0][0] + ";Database=" + companyCode + ";User Id=" + dataTable.Rows[0][2] + ";Password=" + dataTable.Rows[0][3];
            using (SqlConnection connectionExact = new SqlConnection(conn))
            {
                using (SqlCommand commandExact = new SqlCommand(sql, connectionExact))
                {
                    connectionExact.Open();
                    //commandExact.CommandTimeout = 0;
                    commandExact.Parameters.Clear();

                    if (!string.IsNullOrEmpty(creditorNo))
                    {
                        commandExact.Parameters.AddWithValue("@param1", creditorNo);
                    }
                    else
                    {
                        commandExact.Parameters.AddWithValue("@param1", DBNull.Value);
                    }

                    if (!string.IsNullOrEmpty(poNo))
                    {
                        commandExact.Parameters.AddWithValue("@param2", poNo);
                    }
                    else
                    {
                        commandExact.Parameters.AddWithValue("@param2", DBNull.Value);
                    }

                    if (!string.IsNullOrEmpty(invtobereceivedFrom))
                    {
                        commandExact.Parameters.AddWithValue("@param3", invtobereceivedFrom);
                    }
                    else
                    {
                        commandExact.Parameters.AddWithValue("@param3", DBNull.Value);
                    }

                    if (!string.IsNullOrEmpty(invtobereceivedTo))
                    {
                        commandExact.Parameters.AddWithValue("@param4", invtobereceivedTo);
                    }
                    else
                    {
                        commandExact.Parameters.AddWithValue("@param4", DBNull.Value);
                    }
                    SqlDataReader reader = commandExact.ExecuteReader();
                    dtexact.Load(reader);
                    reader.Close();
                }
            }

            var response = req.CreateResponse(HttpStatusCode.OK);
            List<object> rows = new List<object>();
            foreach (DataRow row in dtexact.Rows)
            {
                var rowData = new Dictionary<string, object>();
                foreach (DataColumn column in dtexact.Columns)
                {
                    rowData[column.ColumnName] = row[column];
                }
                rows.Add(rowData);
            }
            await response.WriteAsJsonAsync(rows);
            return response;
        }

        public class GetLoginType
        {
            public string loginemail { get; set; }
        }
    }
}