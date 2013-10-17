/*
 * Created by SharpDevelop.
 * User: Иван
 * Date: 04.02.2011
 * Time: 23:25
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Net;
using System.Data;
using System.IO;
using cbrexporter.www.cbr.ru;

namespace cbrexporter
{
	class Program
	{
		public static void Main(string[] args)
		{
			String filename = null;
			String key = "regions";
			if (args.Length > 0) key = args[0];
			
			CookieContainer cook = new CookieContainer();
			RegionInfoService service = new RegionInfoService();			
			service.CookieContainer = cook;
			service.AllowAutoRedirect = false;
			bool Derr = service.OpenDataBase();
			DataSet ds = null;
			if (key.Equals("regions"))
				ds = service.RegionsList();
			else if (key.Equals("tables"))
				ds = service.TablesList();
			else if (key.Equals("tables"))
				ds = service.TablesList();
			else if (key.Equals("indicators") & args.Length > 1)
				ds = service.IndicatorsList(args[1]);
			else if (key.Equals("values") & args.Length > 2)
			{
				if (args.Length > 3) 
				{
					filename = args[3];
					if (File.Exists(filename) == true)
					{
						return;
					}
				}
				service.AddRegion(System.Int32.Parse(args[1]));
				string[] inds = args[2].Split(',');
				foreach (string ind in inds)
				{
					service.AddIndicators(System.Int32.Parse(ind));
				}
				service.SetDatesRange(new DateTime(2001, 01,01), new DateTime(2014, 01,01));
				ds = service.GetRawRegionData();
				service.ClearIndicatorsList();
				service.ClearRegionList();
				
			}
			
			if (ds != null)
			{
				if (filename == null)
				{
					Console.WriteLine(ds.GetXml().ToString());
				}
				else
				{
					StreamWriter fs = File.CreateText(filename);
					fs.Write(ds.GetXml().ToString());
					fs.Close();							
				}
			}
			service.Close();								
		}
	}
}