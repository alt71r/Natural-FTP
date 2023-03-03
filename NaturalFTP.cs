using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Eventing.Reader;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

public class NaturalFTP
{
    public bool passive = true;

    public IPEndPoint ep;
    public string uname;
    public string passw;

    public event Action<string> OnMessageSent;
    public event Action<string> OnMessageRecv;
    public event Action<bool> OnLoginComplete;
    
    public enum EState : byte
    {
        disconnected = 0,
        connecting = 1,
        connected = 2,
        identified = 3,

        ping = 11,
        pwd = 12,
        cwd = 13,
        type = 14,
        rest = 15,
        quit = 16,
        size = 17,
        dele = 18,
        mdir = 19,
        rdir = 20,

        with_data_conn = 30,

        list = 31,
        stor = 32,
        retr = 33
    }

    bool aborting = false;
    EState state_local = EState.disconnected;
    public event Action<EState> OnStateChanged;
    public EState State
    {
        get => state_local;
        set
        {
            if (state_local == value) return;
            state_local = value;
            OnStateChanged?.Invoke(state_local);
        }
    }

    enum ESubState
    {
        entering_mode,
        entering_type,
        real_command
    }

    ESubState submode;

    Socket sock_service;
    Socket sock_data;

    Thread sock_thread;
    Thread service_thread;
    Thread data_thread;

    IPEndPoint data_ep;

    bool running = false;
    bool service_running = false;
    bool data_running = false;

    StringBuilder sb = new StringBuilder(2000);
    StringBuilder sb_data = new StringBuilder(2000);

    AutoResetEvent evt_data = new AutoResetEvent(false);
    AutoResetEvent evt_port_ready = new AutoResetEvent(false);
    AutoResetEvent evt_data_ended = new AutoResetEvent(false);

    Queue<string> recv_cmds = new Queue<string>(20);

    Timer timer;
    object cOnComplete;
    object cOnItem;
    object cOnError;

    FileStream fs;
    string fname_remote;
    //long filesize;
    long filepos;

    public NaturalFTP()
    {
        timer = new Timer(OnTimeout);
    }

    void OnTimeout(object tmr)
    {
        if (State == EState.connected)
        {
            service_running = false;
        }
        else if (State == EState.identified)
        {
            State = EState.ping;
            SendCommand("NOOP");
        }
        else if (State == EState.ping)
        {
            service_running = false;
        }
        else if (State == EState.stor || State == EState.retr)
        {
            fs.Close();
            if (cOnError is Action<string> acstr) acstr?.Invoke("Timeout");
            service_running = false;
        }
        else
        {
            if (cOnError is Action<string> acstr) acstr?.Invoke("Timeout");
            service_running = false;
        }
    }

    public void Start()
    {
        if (running) return;


        service_running = true;
        service_thread = new Thread(ServiceLoop);
        service_thread.Start();

        running = true;
        sock_thread = new Thread(SockLoop);
        sock_thread.Start();
    }

    public void Stop()
    {
        if (!running) return;

        running = false;
        service_running = false;
    }

    public void GetCurrentDir(Action<string> onComplete, Action<string> onError)
    {
        if (State != EState.identified)
        {
            onError?.Invoke("Client not ready");
            return;
        }

        State = EState.pwd;
        cOnComplete = onComplete;
        cOnError = onError;
        SendCommand("PWD");
        timer.Change(cmd_timeout, -1);
    }

    public enum EMode
    {
        ASCII,
        BIN
    }

    public void ChangeMode(EMode mode, Action onComplete, Action<string> onError)
    {
        if (State != EState.identified)
        {
            onError?.Invoke("Client not ready");
            return;
        }

        State = EState.type;
        cOnComplete = onComplete;
        cOnError = onError;
        if (mode == EMode.ASCII)
            SendCommand("TYPE A");
        else
            SendCommand("TYPE I");
        timer.Change(cmd_timeout, -1);
    }

    public void GetFileSize(string fname, Action<long> onComplete, Action<string> onError)
    {
        if (State != EState.identified)
        {
            onError?.Invoke("Client not ready");
            return;
        }

        State = EState.size;
        cOnComplete = onComplete;
        cOnError = onError;
        SendCommand($"SIZE {fname}");
        timer.Change(cmd_timeout, -1);
    }

    public void DeleteFile(string fname, Action onComplete, Action<string> onError)
    {
        if (State != EState.identified)
        {
            onError?.Invoke("Client not ready");
            return;
        }

        State = EState.dele;
        cOnComplete = onComplete;
        cOnError = onError;
        SendCommand($"DELE {fname}");
        timer.Change(cmd_timeout, -1);
    }

    public void Quit(Action onComplete, Action<string> onError)
    {
        if (State != EState.identified)
        {
            onError?.Invoke("Client not ready");
            return;
        }

        State = EState.quit;
        cOnComplete = onComplete;
        cOnError = onError;
        SendCommand("QUIT");
        timer.Change(cmd_timeout, -1);
    }

    public void ListFiles(Action onComplete, Action<string> onItem, Action<string> onError)
    {
        if (State != EState.identified)
        {
            onError?.Invoke("Client not ready");
            return;
        }

        State = EState.list;
        submode = ESubState.entering_mode;

        //port_state = true;
        cOnComplete = onComplete;
        cOnItem = onItem;
        cOnError = onError;

        timer.Change(cmd_timeout, -1);
        EstablishDataConn();
    }    

    public void ChangeCurrentDir(string dir, Action onComplete, Action<string> onError)
    {
        if (State != EState.identified)
        {
            onError?.Invoke("Client not ready");
            return;
        }

        State = EState.cwd;
        cOnComplete = onComplete;
        cOnError = onError;
        SendCommand($"CWD {dir}");
        timer.Change(cmd_timeout, -1);
    }

    public void CreateDir(string dir, Action onComplete, Action<string> onError)
    {
        if (State != EState.identified)
        {
            onError?.Invoke("Client not ready");
            return;
        }

        State = EState.mdir;
        cOnComplete = onComplete;
        cOnError = onError;
        SendCommand($"MKD {dir}");
        timer.Change(cmd_timeout, -1);
    }

    public void DeleteDir(string dir, Action onComplete, Action<string> onError)
    {
        if (State != EState.identified)
        {
            onError?.Invoke("Client not ready");
            return;
        }

        State = EState.rdir;
        cOnComplete = onComplete;
        cOnError = onError;
        SendCommand($"RMD {dir}");
        timer.Change(cmd_timeout, -1);
    }

    public void SendFile(string fname_local, string fname_remote, Action onComplete, Action<long> onProgress, Action<string> onError)
    {
        if (State != EState.identified)
        {
            onError?.Invoke("Client not ready");
            return;
        }

        State = EState.stor;
        submode = ESubState.entering_mode;

        cOnComplete = onComplete;
        cOnItem = onProgress;
        cOnError = onError;

        this.fname_remote = fname_remote;
        try { fs = File.Open(fname_local, FileMode.Open, FileAccess.Read); }
        catch (Exception e)
        {
            onError?.Invoke(e.Message);
            State = EState.identified;
            return;
        }

        filepos = 0;

        timer.Change(cmd_timeout, -1);
        EstablishDataConn();
    }

    public void RecvFile(string fname_local, string fname_remote, Action onComplete, Action<long> onProgress, Action<string> onError)
    {
        if (State != EState.identified)
        {
            onError?.Invoke("Client not ready");
            return;
        }

        State = EState.retr;
        submode = ESubState.entering_mode;

        cOnComplete = onComplete;
        cOnItem = onProgress;
        cOnError = onError;

        this.fname_remote = fname_remote;
        try { fs = File.Open(fname_local, FileMode.OpenOrCreate, FileAccess.Write); }
        catch { }
        //filesize = 0;
        filepos = 0;

        timer.Change(cmd_timeout, -1);
        EstablishDataConn();
    }

    public void Abort(Action onAbortComplete)
    {
        cOnComplete = onAbortComplete;
        if (State < EState.with_data_conn) return;
        aborting = true;
        data_running = false;
    }

    void EstablishDataConn()
    {
        if (passive) SendCommand("PASV");
        else
        {
            evt_port_ready.Reset();

            data_running = true;
            data_thread = new Thread(DataLoop);
            data_thread.Start();
            timer.Change(cmd_timeout, -1);

            while (running)
                if (evt_port_ready.WaitOne(1000))
                    break;

            if (!running) return;

            var ip = data_ep.Address.GetAddressBytes();
            var p1 = data_ep.Port & 0xFF;
            var p2 = (data_ep.Port >> 8) & 0xFF;
            SendCommand($"PORT {ip[0]},{ip[1]},{ip[2]},{ip[3]},{p2},{p1}");

        }
    }

    const int login_temeout = 5000;
    const int inactivity_period = 20000;
    const int cmd_timeout = 2000;


    void ServiceLoop()
    {
        while (running)
        {
            if (!evt_data.WaitOne(1000))
                continue;

            while (service_running) //process all commands
            {
                string line;
                lock (recv_cmds)
                {
                    if (recv_cmds.Count == 0) break;
                    line = recv_cmds.Dequeue();
                }

                Trace.WriteLine($"{State} {line}");
                OnMessageRecv?.Invoke(line);

                var match = Regex.Match(line, @"^(\d+)[^\d]");
                if (!match.Success) continue;

                int code = int.Parse(match.Groups[1].Value);

                if (State == EState.connected)
                {
                    if (code == 220) //new user
                        SendCommand("USER " + uname);
                    else if (code == 331)
                        SendCommand("PASS " + passw);
                    else if (code == 230)
                    {
                        State = EState.identified;
                        timer.Change(inactivity_period, -1);
                        OnLoginComplete?.Invoke(true);
                    }
                    else if (code == 530)
                    {
                        State = EState.connected;
                        timer.Change(-1, -1);
                        OnLoginComplete?.Invoke(false);
                    }
                }
                else if (State == EState.ping)
                {
                    if (code == 200)
                    {
                        State = EState.identified;
                        timer.Change(inactivity_period, -1);
                    }
                }
                else if (State == EState.pwd)
                {
                    if (code == 257)
                    {
                        State = EState.identified;
                        timer.Change(inactivity_period, -1);

                        match = Regex.Match(line, @"^257 ""(.*)""");
                        if (match.Success)
                        {
                            if (cOnComplete is Action<string> acstr)
                                acstr?.Invoke(match.Groups[1].Value);
                        }
                        else
                        {
                            if (cOnError is Action<string> acstr)
                                acstr?.Invoke("Invalid response");
                        }
                    }
                }
                else if (State == EState.cwd)
                {                    
                    if (code == 250)
                    {
                        State = EState.identified;
                        timer.Change(inactivity_period, -1);

                        if (cOnComplete is Action actcmpl)
                            actcmpl?.Invoke();
                    }
                    else if (code == 550)
                    {
                        State = EState.identified;
                        timer.Change(inactivity_period, -1);

                        if (cOnError is Action<string> acstr)
                            acstr?.Invoke("Invalid path");
                    }
                    else
                    {
                        State = EState.identified;
                        timer.Change(inactivity_period, -1);

                        if (cOnError is Action<string> acstr)
                            acstr?.Invoke("Invalid response");
                    }
                }
                else if (State == EState.type)
                {
                    if (code == 230) { }
                    else if (code == 200)
                    {
                        State = EState.identified;
                        timer.Change(inactivity_period, -1);

                        if (cOnComplete is Action actcmpl)
                            actcmpl?.Invoke();
                    }
                    else
                    {
                        State = EState.identified;
                        timer.Change(inactivity_period, -1);
                        if (cOnError is Action<string> acstr)
                            acstr?.Invoke("Invalid response");
                    }
                }
                else if (State == EState.quit)
                {
                    State = EState.identified;
                    timer.Change(inactivity_period, -1);

                    if (code == 220)
                    {
                        if (cOnComplete is Action actcmpl)
                            actcmpl?.Invoke();
                    }
                    else
                        if (cOnError is Action<string> acstr)
                        acstr?.Invoke("Invalid response");
                }
                else if (State == EState.size)
                {
                    State = EState.identified;
                    timer.Change(inactivity_period, -1);

                    if (code == 213)
                    {
                        match = Regex.Match(line, @"^213 (\d+)$");
                        if (match.Success && long.TryParse(match.Groups[1].Value, out long sz))
                        {
                            if (cOnComplete is Action<long> actlng)
                                actlng?.Invoke(sz);
                        }
                        else
                            if (cOnError is Action<string> acstr)
                            acstr?.Invoke("Invalid response");
                    }
                    else if (code == 550)
                    {
                        if (cOnError is Action<string> acstr)
                            acstr?.Invoke("File not found");
                    }
                    else if (cOnError is Action<string> acstr)
                        acstr?.Invoke("Invalid response");
                }
                else if (State == EState.dele)
                {
                    State = EState.identified;
                    timer.Change(inactivity_period, -1);

                    if (code == 250)
                    {
                        if (cOnComplete is Action actcmpl)
                            actcmpl?.Invoke();
                    }
                    else if (code == 550)
                    {
                        if (cOnError is Action<string> acstr)
                            acstr?.Invoke("File not found");
                    }
                    else if (cOnError is Action<string> acstr)
                        acstr?.Invoke("Invalid response");
                }
                else if (State == EState.mdir)
                {
                    State = EState.identified;
                    timer.Change(inactivity_period, -1);
                    if (code == 257)
                    {
                        if (cOnComplete is Action actcmpl)
                            actcmpl?.Invoke();
                    }
                    else if (code == 550)
                    {
                        if (cOnError is Action<string> acstr)
                            acstr?.Invoke("Operation failed");
                    }
                    else if (cOnError is Action<string> acstr)
                        acstr?.Invoke("Invalid response");
                }
                else if (State == EState.rdir)
                {
                    State = EState.identified;
                    timer.Change(inactivity_period, -1);
                    if (code == 250)
                    {
                        if (cOnComplete is Action actcmpl)
                            actcmpl?.Invoke();
                    }
                    else if (cOnError is Action<string> acstr)
                        acstr?.Invoke("Invalid response");
                }
                else if (State > EState.with_data_conn)
                {
                    if (passive)
                    {
                        if (submode == ESubState.entering_mode)
                        {
                            if (code == 227) //passive mode ok
                            {
                                match = Regex.Match(line, @"^227 .*\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+)\)");
                                if (match.Success)
                                {
                                    uint data_ip = uint.Parse(match.Groups[1].Value);
                                    data_ip += uint.Parse(match.Groups[2].Value) << 8;
                                    data_ip += uint.Parse(match.Groups[3].Value) << 16;
                                    data_ip += uint.Parse(match.Groups[4].Value) << 24;

                                    int data_port = byte.Parse(match.Groups[6].Value);
                                    data_port += byte.Parse(match.Groups[5].Value) << 8;

                                    data_ep = new IPEndPoint(data_ip, data_port);


                                    data_running = true;
                                    data_thread = new Thread(DataLoop);
                                    data_thread.Start();

                                    //port_state = false;

                                    if (State == EState.list) SendCommand($"LIST -a");
                                    else if (State == EState.stor) SendCommand($"STOR {fname_remote}");
                                    else if (State == EState.retr) SendCommand($"RETR {fname_remote}");

                                    submode = ESubState.real_command;
                                    timer.Change(inactivity_period, -1);
                                }
                                else
                                {
                                    State = EState.identified;
                                    timer.Change(inactivity_period, -1);
                                    data_running = false;
                                    if (cOnError is Action<String> actstr) actstr?.Invoke("PASV format");
                                }
                            }
                            else
                            {
                                State = EState.identified;
                                timer.Change(inactivity_period, -1);
                                data_running = false;
                                if (cOnError is Action<String> actstr) actstr?.Invoke("Cannot enter passive mode");
                            }
                        }
                        else
                        {
                            if (code == 150) //opening data conn
                            {
                            }
                            else if (code == 500)
                            {
                                State = EState.identified;
                                timer.Change(inactivity_period, -1);
                                data_running = false;
                                if (cOnError is Action<String> actstr) actstr?.Invoke("Unknown command");
                            }
                            else if (code == 226)
                            {
                                if (!evt_data_ended.WaitOne(3000))
                                {
                                    State = EState.identified;
                                    timer.Change(inactivity_period, -1);
                                    if (cOnError is Action<string> acterr)
                                        acterr?.Invoke("Data thread timed out");
                                    data_running = false;
                                }
                                else if (aborting)
                                {
                                    aborting = false;
                                    State = EState.identified;
                                    timer.Change(inactivity_period, -1);
                                    if (cOnComplete is Action actcmpl)
                                        actcmpl?.Invoke();
                                }
                                else 
                                {
                                    State = EState.identified;
                                    timer.Change(inactivity_period, -1);
                                    if (cOnComplete is Action actcmpl)
                                        actcmpl?.Invoke();
                                }
                                
                            }
                            else  //closing data conn
                            {
                                State = EState.identified;
                                timer.Change(inactivity_period, -1);
                                data_running = false;
                                if (cOnError is Action<String> actstr) actstr?.Invoke("Command failure");
                            }
                        }
                    }
                    else //port mode
                    {
                        if (submode == ESubState.entering_mode)
                        {
                            if (code == 200)
                            {
                                if (State == EState.list) SendCommand($"LIST -a");
                                else if (State == EState.stor) SendCommand($"STOR {fname_remote}");
                                else if (State == EState.retr) SendCommand($"RETR {fname_remote}");

                                submode = ESubState.real_command;
                                timer.Change(cmd_timeout, -1);
                            }
                            else
                            {
                                State = EState.identified;
                                timer.Change(inactivity_period, -1);
                                data_running = false;
                                if (cOnError is Action<String> actstr) actstr?.Invoke("Cannot open data connection");
                            }
                        }
                        else
                        {
                            if (code == 150) //opening data conn
                            {
                                /*match = Regex.Match(cmd, @"\((\d+) bytes\)");
                                if (match.Success)
                                    filesize = long.Parse(match.Groups[1].Value);*/
                            }
                            else if (code == 226)
                            {
                                if (State != EState.list)
                                {
                                    State = EState.identified;
                                    timer.Change(inactivity_period, -1);
                                    if (cOnComplete is Action actcmpl)
                                        actcmpl?.Invoke();
                                }
                            }
                            else
                            {
                                State = EState.identified;
                                timer.Change(cmd_timeout, -1);
                                data_running = false;
                                if (cOnError is Action<String> actstr) actstr?.Invoke("Error command");
                            }
                        }
                    }
                }
            }

            lock (recv_cmds)
                recv_cmds.Clear();
        }
    }

    void DataLoop()
    {
        Socket sock_srv = null;
        evt_data_ended.Reset();

        if (passive)
        {
            sock_data = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try { sock_data.Connect(data_ep); }
            catch (SocketException)
            {
                data_running = false;
                State = EState.identified;
                timer.Change(inactivity_period, -1);
                if (cOnError is Action<string> acterr) acterr?.Invoke("Error establishing data connection");
                return;
            }
        }
        else
        {
            sock_srv = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            sock_srv.Bind(new IPEndPoint(IPAddress.Any, 0));
            data_ep = new IPEndPoint((sock_service.LocalEndPoint as IPEndPoint).Address, (sock_srv.LocalEndPoint as IPEndPoint).Port);
            sock_srv.Listen();

            evt_port_ready.Set();

            while (running && data_running)
            {
                if (sock_srv.Poll(100, SelectMode.SelectRead))
                    break;
            }

            if (!running || !data_running)
            {
                sock_srv.Close();
                return;
            }

            sock_data = sock_srv.Accept();
        }

        sb_data.Clear();
        int breathe = 0;
        string error = "";

        while (running && data_running)
        {
            if (State == EState.stor)
            {
                var fbuf = new byte[2048];
                //fs.Seek(filepos, SeekOrigin.Begin);
                int cnt;
                try { cnt = fs.Read(fbuf, 0, fbuf.Length); }
                catch { error = "Error sending data"; break; }

                try { sock_data.Send(fbuf, cnt, SocketFlags.None); }
                catch (SocketException) { error = "Error sending data"; break; }
                catch (ObjectDisposedException) { error = "Error sending data"; break; }

                filepos += cnt;

                if (cOnItem is Action<long> actdbl)
                    actdbl?.Invoke(filepos);

                if (cnt < fbuf.Length)
                    break;

                timer.Change(cmd_timeout, -1);

                breathe++;
                if (breathe > 500)
                {
                    Thread.Sleep(20);
                    breathe = 0;
                }

                continue;
            }

            if (!sock_data.Poll(1000, SelectMode.SelectRead))
                continue;

            int k = sock_data.Available;

            if (k <= 0)
                break;

            if (k > 2048) k = 2048;

            var buf = new byte[k];

            try { sock_data.Receive(buf); }
            catch (SocketException) { error = "Error recieving data"; break; }
            catch (ObjectDisposedException) { error = "Error recieving data"; break; }

            if (State == EState.retr)
            {
                fs.Write(buf, 0, buf.Length);
                filepos += k;
                if (cOnItem is Action<long> actdbl)
                    actdbl?.Invoke(filepos);
            }

            if (State == EState.list && cOnItem is Action<string> actstr)
            {
                foreach (var b in buf)
                {
                    if (b == 0x0a || b == 0x0d)
                    {
                        if (sb_data.Length == 0) continue;
                        actstr?.Invoke(sb_data.ToString());
                        sb_data.Clear();

                    }
                    else sb_data.Append((char)b);
                }
            }

            timer.Change(cmd_timeout, -1);
        }

        if (!passive)
            sock_srv?.Close();

        try { sock_data.Close(); }
        catch (SocketException) { }
        catch (ObjectDisposedException) { }

        if (State == EState.stor || State == EState.retr)
        {
            try { fs.Close(); }
            catch { }
        }
        
        if (State == EState.list && cOnItem is Action<string> actstr_f && sb_data.Length > 0 && error == "")
            actstr_f?.Invoke(sb_data.ToString());

        data_running = false;

        if (error != "")
        {
            State = EState.identified;
            timer.Change(inactivity_period, -1);

            if (cOnError is Action<string> acterr) 
                acterr?.Invoke(error);
        }
        
        evt_data_ended.Set();
    }

    void SendCommand(string cmd)
    {
        OnMessageSent?.Invoke(cmd);
        try { sock_service.Send(Encoding.ASCII.GetBytes(cmd + "\r\n")); }
        catch (SocketException) { }
        catch (ObjectDisposedException) { }
    }

    void SockLoop()
    {
        while (running)
        {
            State = EState.connecting;

            OnMessageSent?.Invoke($"connecting to {ep.ToString()}...");
            sock_service = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                sock_service.Connect(ep);
            }
            catch (SocketException se)
            {
                OnMessageRecv?.Invoke("cannot connect");
                State = EState.disconnected;
                timer.Change(-1, -1);
                Thread.Sleep(3000);
                service_running = false;
                data_running = false;
                continue;
            }

            OnMessageRecv?.Invoke("connected");
            State = EState.connected;
            timer.Change(login_temeout, -1);
            evt_data.Reset();
            service_running = true;
            sb.Clear();

            while (service_running)
            {
                if (!sock_service.Poll(1000, SelectMode.SelectRead))
                    continue;

                int k = sock_service.Available;
                if (k <= 0) break;

                var buf = new byte[k];
                try { sock_service.Receive(buf); }
                catch (SocketException) { break; }
                catch (ObjectDisposedException) { break; }


                foreach (var b in buf)
                {
                    if (b == 0x0a || b == 0x0d)
                    {
                        if (sb.Length == 0) continue;

                        lock (recv_cmds)
                            recv_cmds.Enqueue(sb.ToString());

                        evt_data.Set();
                        sb.Clear();
                    }
                    else sb.Append((char)b);
                }
            }

            OnMessageRecv?.Invoke("disconnected");

            timer.Change(-1, -1);
            State = EState.disconnected;

            try { sock_service.Close(); }
            catch (SocketException) { }
            catch (ObjectDisposedException) { }

            service_running = false;
            data_running = false;
            Thread.Sleep(1000);
            continue;
        }
    }
}
