Imports ATP_BetfairAPI.DatabseActions
Imports ATP_BetfairAPI.BetfairAPI
Imports System
Imports System.ComponentModel
Imports System.IO
Imports System.Text
Imports System.Data.OleDb
Imports System.Threading
Imports System.Threading.Tasks
Imports System.Globalization
Imports System.Text.RegularExpressions
Imports System.Net
Imports System.Web.Script.Serialization
Imports Newtonsoft.Json
Imports Newtonsoft.Json.Linq
Imports System.Data.SqlClient

Public Module GlobalVariables
    Public SessionToken As String
    Public Appkey As String = "ZfI8hcEMs3uAzPmD"
End Module

Module Module1

    Sub Main()


        Dim database As New DatabseActions
        Dim API As New BetfairAPI
        SessionToken = API.GetSessionKey("ZfI8hcEMs3uAzPmD", "username=00alawre&password=portsmouth1")

        Call GetMarketIDs(SessionToken)


        '-------------------------------------------------------------------------------------------------------------------
        'Pull out market IDs and grab Json data for each market
        'Iterate though market IDs to get horses and odds
        '-------------------------------------------------------------------------------------------------------------------

        Dim Time As String = Format(Date.Now, "HH:mm")
        Dim marketIds As DataTable = database.SELECTSTATEMENT("MarketID", "BetfairMarketIds", "WHERE RaceTime > '" & Time & "'")

        For Each row As DataRow In marketIds.Rows()


            Dim Thread1 As New Threading.Thread(AddressOf BetfairData)
            Thread1.IsBackground = False
            Thread1.Start(row)

            'Call BetfairData(row)



        Next



    End Sub
    Private Sub GetMarketIDs(ByVal SessionToken As String)

        'API Call for MarketId's
        Dim API As New BetfairAPI
        Dim MarketJSON As String = API.CreateRequest("ZfI8hcEMs3uAzPmD", SessionToken)

        'API Call to get today's marketId's
        Dim o As JObject = JObject.Parse(MarketJSON)
        Dim results As List(Of JToken) = o.Children().ToList

        'Declare database class objects
        Dim database As New DatabseActions
        Dim columns As String = "MarketID, Meeting, RaceTime, TotalMatched, MarketName"


        'ProcessJSON into database
        For Each item As JProperty In results
            item.CreateReader()
            'MsgBox(item.Value)
            If item.Value.Type = JTokenType.Array Then
                For Each marketId As JObject In item.Values

                    Dim values As String = ""

                    Dim MID As String = marketId("marketId")
                    Dim UTC As DateTime = marketId("marketStartTime")
                    Dim RaceTime As String = "'" & UTC.ToLocalTime().ToString("HH:mm") & "'"
                    'Dim RaceTime As String = Local.ToString.Split(" ")(1)
                    Dim Meeting As String = "'" & marketId.GetValue("event")("venue").ToString & "'"
                    Dim TotalMatched As Decimal = marketId.GetValue("totalMatched")
                    Dim MarketName As String = "'" & marketId.GetValue("marketName").ToString & "'"




                    values = MID & "," & Meeting & "," & RaceTime & "," & TotalMatched & "," & MarketName

                    'database.INSERT("BetfairMarketIDs", columns, values)

                    Using connection As New SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings("PuntersEdgeDB").ConnectionString)

                        command.CommandText = "INSERT INTO BetfairMarketIDs(" & columns & ")" & " VALUES (" & values & ")"
                        command.Connection = connection

                        connection.Open()
                        command.ExecuteNonQuery()
                        connection.Close()

                    End Using

                Next
            End If
        Next

    End Sub
    Private Sub BetfairData(ByVal row As DataRow)

        Dim database As New DatabseActions
        Dim API As New BetfairAPI


        'Declare marketIDs and get Json for horse names and prices
        Dim marketID As String = row.Item(0).ToString

        Dim horseJson As String = API.GetHorses(Appkey, SessionToken, marketID)

        'Parse Json Files into Lists
        Dim horse_o As JObject = JObject.Parse(horseJson)
        Dim horse_resutls As List(Of JToken) = horse_o.Children().ToList


        'Declare database class objects
        Dim columns As String = ""


        '-------------------------------------------------------------------------------------------------------------------

        'Pull our horse metadata
        For Each selection As JProperty In horse_resutls
            selection.CreateReader()
            If selection.Value.Type = JTokenType.Array Then
                For Each sub_selectionID As JObject In selection.Values

                    Dim UTC As DateTime = sub_selectionID("marketStartTime")
                    Dim Local As DateTime = UTC.ToLocalTime
                    Dim RaceTime As String = "'" & Local.ToString.Split(" ")(1) & "'"


                    Dim runnerdetails As List(Of JToken) = sub_selectionID.Children().ToList


                    For Each sub_runner As JProperty In runnerdetails
                        sub_runner.CreateReader()

                        If sub_runner.Value.Type = JTokenType.Array Then

                            For Each horse As JObject In sub_runner.Values

                                Dim values As String = ""

                                Dim selectionID As String = horse("selectionId")
                                Dim runnerName As String = "'" & horse("runnerName").ToString & "'"

                                columns = "SelectionID, MarketID, RaceTime, Horse, StartingPrice"
                                values = selectionID & "," & marketID & "," & RaceTime & "," & runnerName & ", 0"

                                Using con As New SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings("PuntersEdgeDB").ConnectionString)

                                    Dim comm As New SqlCommand

                                    Try

                                        con.Open()
                                        comm.CommandText = "INSERT INTO BetfairData(" & columns & ")" & " VALUES (" & values & ")"
                                        comm.Connection = con
                                        comm.ExecuteNonQuery()
                                        con.Close()

                                    Catch ex As Exception

                                        GoTo NextHorse

                                    End Try


                                End Using


NextHorse:
                            Next

                        End If

                    Next

                Next

            End If

        Next


        Call BetfairOddsUpdater(row)


    End Sub

    Private Sub BetfairOddsUpdater(ByVal row As DataRow)

        Dim database As New DatabseActions
        Dim API As New BetfairAPI


        'Declare marketIDs and get Json for horse names and prices
        Dim marketID As String = row.Item(0).ToString
        Dim priceJson As String = API.GetPrices(Appkey, SessionToken, marketID)

        Dim o As JObject = JObject.Parse(priceJson)
        Dim results As List(Of JToken) = o.Children().ToList

        For Each item As JProperty In results
            item.CreateReader()

            If item.Value.Type = JTokenType.Array Then
                For Each selectionID As JObject In item.Values



                    Dim Matched As String = selectionID("totalMatched")


                    Dim runners As List(Of JToken) = selectionID.Children().ToList

                    For Each runner As JProperty In runners
                        runner.CreateReader()

                        If runner.Value.Type = JTokenType.Array Then

                            For Each horse As JObject In runner.Values

                                Dim values As String = ""

                                Dim Status As String = horse("status")

                                If Not Status = "REMOVED" Then

                                    Dim lastprice As Decimal = 0
                                    Dim selection_TotalMatched As Decimal = 0
                                    Dim selection As String = horse("selectionId")

                                    If Not IsNothing(horse("lastPriceTraded")) Then

                                        lastprice = horse("lastPriceTraded")

                                    Else
                                        lastprice = 0

                                    End If

                                    If Not IsNothing(horse("totalMatched")) Then

                                        selection_TotalMatched = horse("totalMatched")

                                    Else

                                        selection_TotalMatched = 0

                                    End If



                                    Using con As New SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings("PuntersEdgeDB").ConnectionString)

                                            Dim comm As New SqlCommand
                                            comm.CommandText = "UPDATE BetFairData SET 
                                                            Market_TotalMatched = " & Matched & ",
                                                            selection_TotalMatched = " & selection_TotalMatched & ",
                                                            StartingPrice = " & lastprice & "
                                                            WHERE SelectionID = " & selection & "
                                                             AND MarketID = " & marketID

                                            comm.Connection = con

                                            con.Open()
                                            comm.ExecuteNonQuery()
                                            con.Close()

                                        End Using


                                    Else


                                    End If


                            Next

                        End If

                    Next

                Next

            End If

        Next

    End Sub 'Updates the betfairData table with odds and money data (called by first scrape and update)                                       ***** COMPLETE *****
End Module
