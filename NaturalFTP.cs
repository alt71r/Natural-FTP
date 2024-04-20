using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Threading;

public class NaturalFTP
{
    public class FtpException : Exception { public FtpException(string msg) : base(msg) { } }
    public bool PassiveMode { get; set; } = true;
    public bool Connected { get; private set; } = false;

    Socket? sock;
    Socket? data_sock;

    CancellationTokenSource cancel = new CancellationTokenSource();
    public void Cancel() => cancel?.Cancel();

    DispatcherTimer timer;
    public async Task Open(IPEndPoint ep, string uname, string passw)
    {
        if (Connected)
            return;

        if (string.IsNullOrEmpty(uname) && string.IsNullOrEmpty(passw))
        {
            uname = "anonymous";
            passw = "anonymous@gmail.com";
        }

        timer = new DispatcherTimer();
        timer.Interval = TimeSpan.FromSeconds(5);
        timer.Tick += Timer_Tick;

        cancel = new CancellationTokenSource();

        sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await sock.ConnectAsync(ep, cancel.Token).AsTask()
            .WaitAsync(TimeSpan.FromSeconds(5), cancel.Token);

        Connected = true;
        sb.Clear();

        try
        {
            var res = await recvCode();
            if (res.code != 220) throw new FtpException("invalid responce");
            await sendCmd("USER " + uname);
            res = await recvCode();
            if (res.code != 331) throw new FtpException(res.msg);
            await sendCmd("PASS " + passw);
            res = await recvCode();
            if (res.code != 230) throw new FtpException(res.msg);
            await sendCmd("TYPE I");
            res = await recvCode();
            if (res.code != 200) throw new FtpException(res.msg);

            timer.Start();
        }
        catch (Exception)
        {
            try { sock.Close(); } catch { }
            Connected = false;
            throw;
        }
    }

    async void Timer_Tick(object? sender, EventArgs e)
    {
        if (!Connected)
        {
            timer.Stop();
            return;
        }

        try
        {
            await sendCmd("NOOP");
            await recvCode();
        }
        catch { }
    }

    public async Task Close()
    {
        timer?.Stop();
        if (!Connected)
            return;

        await sendCmd("QUIT");
        var res = await recvCode();

        sock?.Close();
        Connected = false;

        if (res.code != 221)
            throw new FtpException("invalid reply");
    }
    public async Task<string> Pwd()
    {
        if (!Connected) throw new FtpException("not connected");
        timer.Stop();
        cancel = new CancellationTokenSource();
        await sendCmd("PWD");
        var res = await recvCode();
        timer.Start();
        if (res.code != 257)
            throw new FtpException(res.msg);
        var match = Regex.Match(res.msg, "^\"([^\"]+)\"");
        if (match.Success)
            return match.Groups[1].Value;
        throw new FtpException("invalid reply");
    }
    public async Task Cwd(string path)
    {
        if (!Connected) throw new FtpException("not connected");
        timer.Stop();
        cancel = new CancellationTokenSource();
        await sendCmd("CWD " + path);
        var ln = await recvCode();
        timer.Start();
        if (ln.code == 250) return;
        if (ln.code == 550) throw new FtpException("invalid dir");
        throw new FtpException("invalid reply");
    }
    public async IAsyncEnumerable<string> ListFiles()
    {
        if (!Connected) throw new FtpException("not connected");
        timer.Stop();
        cancel = new CancellationTokenSource();
        await establish_data_conn();

        await sendCmd("LIST -a");
        var res = await recvCode();
        if (res.code != 150)
        {
            timer.Start();
            throw new FtpException("invalid reply");
        }

        var sbln = new StringBuilder();
        var buf = new byte[2048];


        while (true)
        {
            int cnt;
            try
            {
                cnt = await data_sock!.ReceiveAsync(buf, SocketFlags.None, cancel.Token).AsTask()
                        .WaitAsync(TimeSpan.FromSeconds(5), cancel.Token);
            }
            catch { break; }

            if (cnt == 0)
                break;

            for (int i = 0; i < cnt; i++)
            {
                if (buf[i] == 0x0d || buf[i] == 0x0a)
                {
                    if (sbln.Length > 0)
                    {
                        yield return sbln.ToString();
                        sbln.Clear();
                    }
                }
                else
                    sbln.Append((char)buf[i]);
            }
        }

        data_sock?.Close();

        res = await recvCode();

        timer.Start();
        if (res.code != 226) throw new FtpException("invalid reply");
    }
    public async Task Download(string remotefile, int bufsize, Action<byte[]> progress)
    {
        if (!Connected) throw new FtpException("not connected");
        timer.Stop();
        cancel = new CancellationTokenSource();

        await establish_data_conn();

        await sendCmd("RETR " + remotefile);
        var res = await recvCode();
        if (res.code != 150)
        {
            timer.Start();
            throw new FtpException("invalid reply");
        }

        var buf = new byte[bufsize];
        long sz = 0;
        try
        {
            while (true)
            {
                var cnt = await data_sock!.ReceiveAsync(buf, SocketFlags.None, cancel.Token).AsTask()
                        .WaitAsync(TimeSpan.FromSeconds(5), cancel.Token);

                if (cnt == 0)
                    break;

                progress!.Invoke(buf.Take(cnt).ToArray());

                sz += cnt;
            }
        }
        catch { throw; }
        finally
        {
            try
            {
                data_sock?.Close();
                res = await recvCode();
                if (res.code != 226) throw new FtpException("invalid reply");
            }
            finally
            {
                timer.Start();
            }
        }
    }
    public async Task Upload(string remotefile, int bufsize, Func<long, int, byte[]> getdata)
    {
        if (!Connected) throw new FtpException("not connected");
        timer.Stop();
        cancel = new CancellationTokenSource();

        long sz = 0;

        await establish_data_conn();

        await sendCmd("STOR " + remotefile);
        var res = await recvCode();
        if (res.code != 150)
        {
            timer.Start();
            throw new FtpException("invalid reply");
        }

        try
        {
            while (true)
            {
                var buf = getdata(sz, bufsize);

                if (buf.Length == 0)
                    break;

                await data_sock!.SendAsync(buf, SocketFlags.None, cancel.Token).AsTask()
                        .WaitAsync(TimeSpan.FromSeconds(5), cancel.Token);

                sz += buf.Length;

                if (buf.Length < bufsize)
                    break;

                if (cancel.Token.IsCancellationRequested)
                    break;
            }

            data_sock?.Shutdown(SocketShutdown.Send);
        }
        catch { throw; }
        finally
        {
            try
            {
                data_sock?.Close();
                res = await recvCode();
                if (res.code != 226) throw new FtpException("invalid reply");
            }
            finally
            {
                timer.Start();
            }
        }
    }
    public async Task<long> GetSize(string remotefile)
    {
        if (!Connected) throw new FtpException("not connected");
        timer.Stop();
        await sendCmd("SIZE " + remotefile);
        var res = await recvCode();
        timer.Start();
        if (res.code == 550) throw new FtpException("does not exist");
        if (res.code != 213) throw new FtpException("invalid reply");
        var match = Regex.Match(res.msg, @"^(\d+)$");
        if (match.Success)
            return int.Parse(match.Groups[1].Value);
        throw new FtpException("invalid reply");
    }

    public async Task MakeDir(string dir)
    {
        if (!Connected) throw new FtpException("not connected");
        timer.Stop();
        await sendCmd("MKD " + dir);
        var res = await recvCode();
        timer.Start();
        if (res.code == 550 && res.msg.ToLower().Contains("exist")) return;
        if (res.code != 257) throw new FtpException("invalid reply");
    }

    public async Task DelDir(string dir)
    {
        if (!Connected) throw new FtpException("not connected");
        timer.Stop();
        await sendCmd("RMD " + dir);
        var res = await recvCode();
        timer.Start();
        if (res.code != 250) throw new FtpException("invalid reply");
    }

    public async Task DelFile(string dir)
    {
        if (!Connected) throw new FtpException("not connected");
        timer.Stop();
        await sendCmd("DELE " + dir);
        var res = await recvCode();
        timer.Start();
        if (res.code != 250) throw new FtpException("invalid reply");
    }

    public async Task<long> FileSize(string fname)
    {
        if (!Connected) throw new FtpException("not connected");
        timer.Stop();
        await sendCmd("SIZE " + fname);
        var res = await recvCode();
        timer.Start();
        if (res.code != 213) throw new FtpException("invalid reply");
        return long.Parse(res.msg);
    }

    public async Task Rename(string oldname, string newname)
    {
        if (!Connected) throw new FtpException("not connected");
        timer.Stop();
        await sendCmd("RNFR " + oldname);
        var res = await recvCode();
        if (res.code != 350) throw new FtpException("invalid reply");
        await sendCmd("RNTO " + newname);
        res = await recvCode();
        if (res.code != 250) throw new FtpException("invalid reply");
    }

    #region helper functions
    async Task establish_data_conn()
    {
        /*await sendCmd("TYPE I");
        var res = await recvCode();
        if (res.code != 200) throw new FtpException("invalid reply");*/

        if (PassiveMode)
        {
            await sendCmd("PASV");
            var res = await recvCode();
            if (res.code != 227) throw new FtpException("invalid reply");

            var match = Regex.Match(res.msg, @"\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+)\).?$");
            if (!match.Success) throw new FtpException("invalid reply");

            uint data_ip = uint.Parse(match.Groups[1].Value);
            data_ip += uint.Parse(match.Groups[2].Value) << 8;
            data_ip += uint.Parse(match.Groups[3].Value) << 16;
            data_ip += uint.Parse(match.Groups[4].Value) << 24;

            int data_port = byte.Parse(match.Groups[6].Value);
            data_port += byte.Parse(match.Groups[5].Value) << 8;

            var data_ep = new IPEndPoint(data_ip, data_port);

            data_sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                await data_sock.ConnectAsync(data_ep, cancel.Token).AsTask()
                    .WaitAsync(TimeSpan.FromSeconds(5), cancel.Token);
            }
            catch (Exception ex)
            {

            }
        }
        else
        {
            var sock_srv = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            sock_srv.Bind(new IPEndPoint(IPAddress.Any, 0));
            sock_srv.Listen();

            if (sock?.LocalEndPoint is IPEndPoint localip && sock_srv.RemoteEndPoint is IPEndPoint remoteip)
            {
                var data_ep = new IPEndPoint(localip.Address, remoteip.Port);
                var ip = data_ep.Address.GetAddressBytes();
                var p1 = data_ep.Port & 0xFF;
                var p2 = (data_ep.Port >> 8) & 0xFF;

                await sendCmd($"PORT {ip[0]},{ip[1]},{ip[2]},{ip[3]},{p2},{p1}");
            }
            else
                throw new FtpException("Invalid TCP bind");
        }
    }

    byte[] dummy = new byte[2048];
    async Task sendCmd(string cmd)
    {
        sb.Clear();

        cancel = new CancellationTokenSource();

        try
        {
            //flush
            while (sock?.Available > 0)
                await sock.ReceiveAsync(dummy, SocketFlags.None, cancel.Token).AsTask()
                    .WaitAsync(TimeSpan.FromSeconds(5), cancel.Token);
        }
        catch (Exception ex) when (ex is SocketException || ex is ObjectDisposedException)
        {
            Connected = false;
            timer.Stop();
            throw;
        }

        Trace.WriteLine("<-" + cmd);
        var buf = Encoding.ASCII.GetBytes(cmd + "\r\n");

        try
        {
            if (sock == null)
                return;

            await sock!.SendAsync(buf, SocketFlags.None, cancel.Token).AsTask()
                .WaitAsync(TimeSpan.FromSeconds(5), cancel.Token);//exception
        }
        catch (Exception ex) when (ex is SocketException || ex is ObjectDisposedException)
        {
            Connected = false;
            timer.Stop();
            throw;
        }
    }
    async Task<(int code, string msg)> recvCode()
    {
        cancel = new CancellationTokenSource();

        while (true)
        {
            var ln = await recvLine();
            var match = Regex.Match(ln, @"^(\d+)[\s-]+(.*)$");
            if (match.Success)
                return (int.Parse(match.Groups[1].Value), match.Groups[2].Value);
        }
    }

    StringBuilder sb = new StringBuilder();
    async Task<string> recvLine()
    {
        var buf = new byte[512];

        while (true)
        {
            int cnt;
            try
            {
                cnt = await sock!.ReceiveAsync(buf, SocketFlags.None, cancel.Token).AsTask()
                    .WaitAsync(TimeSpan.FromSeconds(5), cancel.Token);
            }
            catch (Exception ex) when (ex is SocketException || ex is ObjectDisposedException)
            {
                Connected = false;
                timer.Stop();
                throw;
            }
            catch
            {
                sb.Clear();
                throw;
            }


            if (cnt == 0)
            {
                Connected = false;
                timer.Stop();
                throw new SocketException((int)SocketError.ConnectionAborted);
            }

            for (int i = 0; i < cnt; i++)
            {
                if (buf[i] == 0x0d || buf[i] == 0x0a)
                {
                    if (sb.Length > 0)
                    {
                        Trace.WriteLine(":" + sb.ToString());
                        string res = sb.ToString();
                        sb.Clear();
                        return res;
                    }
                }
                else
                    sb.Append((char)buf[i]);
            }
        }

    }
    #endregion
}
