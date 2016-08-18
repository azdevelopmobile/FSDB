using System;
using System.Collections.Generic;
using FSDB.Core;
using Xamarin.Forms;
using System.Threading;
using System.Threading.Tasks;

namespace FSDB
{
    public class Person : FSTableModel
    {
        public int ID { get; set; }
        public string FName { get; set; }
        public string LName { get; set; }
        public string Address { get; set; }
    }
    public class TestPage : ContentPage
    {

        public TestPage()
        {
            Title = "Test";
            var btn = new Button()
            {
                Text = "Insert Parallel",
                Command = new Command(async () =>
                {
                    var list = BuildList();
                    await FSData.Db.AddOrUpdate<Person>(list);
                    var c = await FSData.Db.GetAll<Person>();
                    Console.WriteLine($"Total count read is {c.Count}");
                    Console.WriteLine("Insert completed");

                })
            };
            var btnRead = new Button()
            {
                Text = "Read data",
                Command = new Command(async () =>
                {
                    var list = BuildList();
                    await Task.Run(() =>
                    {
                        Parallel.ForEach(list, async (obj) =>
                        {
                            await FSData.Db.AddOrUpdate<Person>(obj);
                            var c = await FSData.Db.GetAll<Person>();
                            Console.WriteLine($"Total count read is {c.Count}");
                        });
                    });

                })
            };

            Content = new StackLayout()
            {
                Children = { btn, btnRead }
            };
        }

        private List<Person> BuildList()
        {
            var list = new List<Person>();
            for (int x = 0; x < 10000; x++)
            {
                var p = new Person()
                {
                    ID = x,
                    FName = "Name " + x.ToString(),
                    LName = "Last " + x.ToString(),
                    Address = $"{x} East {x} Ave"
                };
                list.Add(p);
            }
            return list;
        }


    }
    public class App : Application
    {
        public App()
        {
            //InitDatabase();

            MainPage = new NavigationPage(new TestPage());
        }

        protected override void OnStart()
        {
            InitDatabase();
            // Handle when your app starts
        }

        protected override void OnSleep()
        {
            FSData.Stop();
        }

        protected override void OnResume()
        {
            InitDatabase();
            // Handle when your app resumes
        }

        private void InitDatabase()
        {
            FSData.Init(new FSConfig()
            {
                DbName = "FSDB",
                IdleTime = 5,
                Settings = {
                    new TableSetting() { PK = "ID", Type = "Person" }
                }

            });
        }
    }

}

