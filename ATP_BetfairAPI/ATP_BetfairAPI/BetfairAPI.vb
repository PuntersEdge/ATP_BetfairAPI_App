Imports System.Web
Imports System.Net
Imports System.Web.Script.Serialization
Imports System.IO
Imports System.Data.SqlClient
Imports System.Text
Imports ATP_BetfairAPI.DatabseActions



Public Class BetfairAPI
    Implements IHttpModule

    Private WithEvents _context As HttpApplication

    ''' <summary>
    '''  You will need to configure this module in the Web.config file of your
    '''  web and register it with IIS before being able to use it. For more information
    '''  see the following link: http://go.microsoft.com/?linkid=8101007
    ''' </summary>
#Region "IHttpModule Members"

    Public Sub Dispose() Implements IHttpModule.Dispose

        ' Clean-up code here

    End Sub

    Public Sub Init(ByVal context As HttpApplication) Implements IHttpModule.Init
        _context = context
    End Sub

#End Region

    Public Sub OnLogRequest(ByVal source As Object, ByVal e As EventArgs) Handles _context.LogRequest

        ' Handles the LogRequest event to provide a custom logging 
        ' implementation for it

    End Sub

    Public Function GetSessionKey(AppKey As String, postData As String)

        Dim con As New SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings("PuntersEdgeSessionKey").ConnectionString)
        Dim command As New SqlCommand
        Dim SessionKey As String

        command.CommandType = CommandType.Text
        command.CommandText = "SELECT SessionToken FROM BetfairSessionToken"
        command.Connection = con
        con.Open()
        SessionKey = command.ExecuteScalar
        con.Close()

        If SessionKey = "-1" Then

            System.Net.ServicePointManager.Expect100Continue = False
            Dim Url As String = "https://identitysso.betfair.com/api/login"
            Dim request As HttpWebRequest = Nothing
            Dim dataStream As Stream = Nothing
            Dim response As WebResponse = Nothing
            Dim strResponseStatus As String = ""
            Dim reader As StreamReader = Nothing
            Dim responseFromServer As String = ""
            Try
                request = WebRequest.Create(New Uri(Url))
                request.Method = "POST"
                request.ContentType = "application/x-www-form-urlencoded"
                request.Accept = "application/json"
                'request.Headers.Add("Accept", "application/json")
                request.Headers.Add("X-Application", AppKey)
                'request.Headers.Add("X-Authentication", SessToken)
                '~~> Data to post such as ListEvents, ListMarketCatalogue etc
                Dim byteArray As Byte() = Encoding.UTF8.GetBytes(postData)
                '~~> Set the ContentLength property of the WebRequest.
                request.ContentLength = byteArray.Length
                '~~> Get the request stream.
                dataStream = request.GetRequestStream()
                '~~> Write the data to the request stream.
                dataStream.Write(byteArray, 0, byteArray.Length)
                '~~> Close the Stream object.
                dataStream.Close()
                '~~> Get the response.
                response = request.GetResponse()
                '~~> Display the status below if required
                '~~> Dim strStatus as String = CType(response, HttpWebResponse).StatusDescription
                strResponseStatus = CType(response, HttpWebResponse).StatusDescription
                '~~> Get the stream containing content returned by the server.
                dataStream = response.GetResponseStream()
                '~~> Open the stream using a StreamReader for easy access.
                reader = New StreamReader(dataStream)
                '~~> Read the content.
                responseFromServer = reader.ReadToEnd()
                '~~> Display the content below if required
                '~~>Dim strShowResponse as String = responseFromServer  '~~>If required
            Catch ex As Exception
                '~~> Show any errors in this method for an error log etc Just use a messagebox for now
                MsgBox("CreateRequest Error" & vbCrLf & ex.Message)
            End Try

            Dim j As Object = New JavaScriptSerializer().Deserialize(Of Object)(responseFromServer)
            SessionKey = j("token")


            '~~> Clean up the streams.
            reader.Close()
            dataStream.Close()
            response.Close()

            Dim dbactions As New DatabseActions
            dbactions.UPDATESession("BetfairSessionToken", SessionKey)

        End If

        Return SessionKey   '~~> Function Output

    End Function '~~>Gets session id for API
    Public Function CreateRequest(AppKey As String, SessToken As String)

        '*** here is the Requst String for you to play around with the parameters

        Dim today As String = Format(Date.Today, "yyyy-MM-dd")
        Dim begintime As String = Format(Date.Now.AddMinutes(10), "HH:mm:ss")
        Dim daterange As String = """from"":""" & today & "T" & begintime & "Z" & """,""to"":""" & today & "T23:45:00Z"""

        Dim postData As String = "{""jsonrpc"": ""2.0"",""method"":""SportsAPING/v1.0/listMarketCatalogue"",""params"":{""filter"":{""eventTypeIds"":[""7""],""marketCountries"":[""GB"",""IE""] ,""marketTypeCodes"":[""WIN""],""marketStartTime"":{" & daterange & "}},""venues"":[],""sort"":""FIRST_TO_START"",""maxResults"":50,""marketProjection"":[""MARKET_START_TIME"", ""EVENT""],""marketStatus"":""OPEN"" },""id"": 1}"

        System.Net.ServicePointManager.Expect100Continue = False
        Dim Url As String = "https://api.betfair.com/exchange/betting/json-rpc/v1/"
        Dim request As WebRequest = Nothing
        Dim dataStream As Stream = Nothing
        Dim response As WebResponse = Nothing
        Dim strResponseStatus As String = ""
        Dim reader As StreamReader = Nothing
        Dim responseFromServer As String = ""
        Try
            request = WebRequest.Create(New Uri(Url))
            request.Method = "POST"
            request.ContentType = "application/json-rpc"
            request.Headers.Add(HttpRequestHeader.AcceptCharset, "ISO-8859-1,utf-8")
            request.Headers.Add("X-Application", AppKey)
            request.Headers.Add("X-Authentication", SessToken)
            '~~> Data to post such as ListEvents, ListMarketCatalogue etc
            Dim byteArray As Byte() = Encoding.UTF8.GetBytes(postData)
            '~~> Set the ContentLength property of the WebRequest.
            request.ContentLength = byteArray.Length
            '~~> Get the request stream.
            dataStream = request.GetRequestStream()
            '~~> Write the data to the request stream.
            dataStream.Write(byteArray, 0, byteArray.Length)
            '~~> Close the Stream object.
            dataStream.Close()
            '~~> Get the response.
            response = request.GetResponse()
            '~~> Display the status below if required
            '~~> Dim strStatus as String = CType(response, HttpWebResponse).StatusDescription
            strResponseStatus = CType(response, HttpWebResponse).StatusDescription
            '~~> Get the stream containing content returned by the server.
            dataStream = response.GetResponseStream()
            '~~> Open the stream using a StreamReader for easy access.
            reader = New StreamReader(dataStream)
            '~~> Read the content.
            responseFromServer = reader.ReadToEnd()
            '~~> Display the content below if required
            '~~>Dim strShowResponse as String = responseFromServer  '~~>If required
        Catch ex As Exception
            '~~> Show any errors in this method for an error log etc Just use a messagebox for now
            MsgBox("CreateRequest Error" & vbCrLf & ex.Message)
        End Try
        Return responseFromServer   '~~> Function Output
        '~~> Clean up the streams.
        reader.Close()
        dataStream.Close()
        response.Close()

    End Function '~~>Gets market ID's of races - Need to modify for IRELAND and add merket ids ito DB
    Public Function GetPrices(AppKey As String, SessToken As String, MarketId As String)

        '*** here is the Requst String for you to play around with the parameters

        Dim today As String = Format(Date.Today, "yyyy-MM-dd")
        Dim begintime As String = Format(Date.Now.AddMinutes(10), "HH:mm:ss")
        Dim daterange As String = """from"":""" & today & "T" & begintime & "Z" & """,""to"":""" & today & "T23:45:00Z"""

        Dim postData As String = "{""jsonrpc"": ""2.0"", ""method"": ""SportsAPING/v1.0/listMarketBook"", ""params"": {""marketIds"":[""" & MarketId & """],""priceProjection"":{""priceData"":[""EX_BEST_OFFERS""]}}, ""id"": 1}"

        System.Net.ServicePointManager.Expect100Continue = False
        Dim Url As String = "https://api.betfair.com/exchange/betting/json-rpc/v1/"
        Dim request As WebRequest = Nothing
        Dim dataStream As Stream = Nothing
        Dim response As WebResponse = Nothing
        Dim strResponseStatus As String = ""
        Dim reader As StreamReader = Nothing
        Dim responseFromServer As String = ""
        Try
            request = WebRequest.Create(New Uri(Url))
            request.Method = "POST"
            request.ContentType = "application/json-rpc"
            request.Headers.Add(HttpRequestHeader.AcceptCharset, "ISO-8859-1,utf-8")
            request.Headers.Add("X-Application", AppKey)
            request.Headers.Add("X-Authentication", SessToken)
            '~~> Data to post such as ListEvents, ListMarketCatalogue etc
            Dim byteArray As Byte() = Encoding.UTF8.GetBytes(postData)
            '~~> Set the ContentLength property of the WebRequest.
            request.ContentLength = byteArray.Length
            '~~> Get the request stream.
            dataStream = request.GetRequestStream()
            '~~> Write the data to the request stream.
            dataStream.Write(byteArray, 0, byteArray.Length)
            '~~> Close the Stream object.
            dataStream.Close()
            '~~> Get the response.
            response = request.GetResponse()
            '~~> Display the status below if required
            '~~> Dim strStatus as String = CType(response, HttpWebResponse).StatusDescription
            strResponseStatus = CType(response, HttpWebResponse).StatusDescription
            '~~> Get the stream containing content returned by the server.
            dataStream = response.GetResponseStream()
            '~~> Open the stream using a StreamReader for easy access.
            reader = New StreamReader(dataStream)
            '~~> Read the content.
            responseFromServer = reader.ReadToEnd()
            '~~> Display the content below if required
            '~~>Dim strShowResponse as String = responseFromServer  '~~>If required
        Catch ex As Exception
            '~~> Show any errors in this method for an error log etc Just use a messagebox for now
            MsgBox("CreateRequest Error" & vbCrLf & ex.Message)
        End Try
        Return responseFromServer   '~~> Function Output
        '~~> Clean up the streams.
        reader.Close()
        dataStream.Close()
        response.Close()

    End Function
    Public Function GetHorses(AppKey As String, SessToken As String, MarketId As String)

        '*** here is the Requst String for you to play around with the parameters

        Dim today As String = Format(Date.Today, "yyyy-MM-dd")
        Dim begintime As String = Format(Date.Now.AddMinutes(10), "HH:mm:ss")
        Dim daterange As String = """from"":""" & today & "T" & begintime & "Z" & """,""to"":""" & today & "T23:45:00Z"""

        Dim postData As String = "{""jsonrpc"": ""2.0"",""method"":""SportsAPING/v1.0/listMarketCatalogue"",""params"":{""filter"":{""marketIds"":[""" & MarketId & """]},""marketProjection"":[""MARKET_START_TIME"", ""RUNNER_DESCRIPTION""],""maxResults"":10 },""id"": 1}"
        'Dim postData As String = "{""jsonrpc"": ""2.0"", ""method"": ""SportsAPING/v1.0/listMarketCatalogue"", ""params"": {""marketIds"":[""" & MarketId & """],""filter"":{""maxResults"":50,""marketProjection"":[""MARKET_START_TIME"", ""RUNNER_METADATA""]}}, ""id"": 1}"

        System.Net.ServicePointManager.Expect100Continue = False
        Dim Url As String = "https://api.betfair.com/exchange/betting/json-rpc/v1/"
        Dim request As WebRequest = Nothing
        Dim dataStream As Stream = Nothing
        Dim response As WebResponse = Nothing
        Dim strResponseStatus As String = ""
        Dim reader As StreamReader = Nothing
        Dim responseFromServer As String = ""
        Try
            request = WebRequest.Create(New Uri(Url))
            request.Method = "POST"
            request.ContentType = "application/json-rpc"
            request.Headers.Add(HttpRequestHeader.AcceptCharset, "ISO-8859-1,utf-8")
            request.Headers.Add("X-Application", AppKey)
            request.Headers.Add("X-Authentication", SessToken)
            '~~> Data to post such as ListEvents, ListMarketCatalogue etc
            Dim byteArray As Byte() = Encoding.UTF8.GetBytes(postData)
            '~~> Set the ContentLength property of the WebRequest.
            request.ContentLength = byteArray.Length
            '~~> Get the request stream.
            dataStream = request.GetRequestStream()
            '~~> Write the data to the request stream.
            dataStream.Write(byteArray, 0, byteArray.Length)
            '~~> Close the Stream object.
            dataStream.Close()
            '~~> Get the response.
            response = request.GetResponse()
            '~~> Display the status below if required
            '~~> Dim strStatus as String = CType(response, HttpWebResponse).StatusDescription
            strResponseStatus = CType(response, HttpWebResponse).StatusDescription
            '~~> Get the stream containing content returned by the server.
            dataStream = response.GetResponseStream()
            '~~> Open the stream using a StreamReader for easy access.
            reader = New StreamReader(dataStream)
            '~~> Read the content.
            responseFromServer = reader.ReadToEnd()
            '~~> Display the content below if required
            '~~>Dim strShowResponse as String = responseFromServer  '~~>If required
        Catch ex As Exception
            '~~> Show any errors in this method for an error log etc Just use a messagebox for now
            MsgBox("CreateRequest Error" & vbCrLf & ex.Message)
        End Try
        Return responseFromServer   '~~> Function Output
        '~~> Clean up the streams.
        reader.Close()
        dataStream.Close()
        response.Close()

    End Function

End Class
