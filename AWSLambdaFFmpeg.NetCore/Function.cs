using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.Lambda.Serialization.Json;
using Amazon.S3;
using Amazon.S3.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(JsonSerializer))]

namespace AWSLambdaFFmpeg.NetCore
{
    public class Function
    {
        private IAmazonS3 S3Client { get; set; }
        public string PostCmd { get; set; }

        public string PreCmd { get; set; }

        public string OptsPara { get; set; }

        public string FilterPara { get; set; }

        public string AudioEncPara { get; set; }

        public string VideoEncPara { get; set; }

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            S3Client = new AmazonS3Client();

            InitEnvVar();
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        /// <param name="s3Client"></param>
        public Function(IAmazonS3 s3Client)
        {
            S3Client = s3Client;
        }

        /// <summary>
        /// Init variables from environment variables
        /// </summary>
        private void InitEnvVar()
        {
            VideoEncPara = Environment.GetEnvironmentVariable("V_ENC_PARA");
            if (string.IsNullOrWhiteSpace(VideoEncPara))
                VideoEncPara = "-c:v libx264 -pix_fmt yuv420p -profile:v high -level 4.0 -b:v 5000K";

            AudioEncPara = Environment.GetEnvironmentVariable("A_ENC_PARA");
            if (string.IsNullOrWhiteSpace(AudioEncPara))
                AudioEncPara = "-c:a aac -b:a 256K";

            FilterPara = Environment.GetEnvironmentVariable("FILTER_PARA");
            if (string.IsNullOrWhiteSpace(FilterPara))
                FilterPara = "-vf \"yadif=0:-1:0,scale=1920:1080\" -r 30";

            OptsPara = Environment.GetEnvironmentVariable("OPTS_PARA");
            if (string.IsNullOrWhiteSpace(OptsPara))
                OptsPara = "-movflags +faststart";

            PreCmd = Environment.GetEnvironmentVariable("PRE_CMD");
            if (string.IsNullOrWhiteSpace(PreCmd))
                PreCmd = "ls -al /tmp";

            PostCmd = Environment.GetEnvironmentVariable("POST_CMD");
            if (string.IsNullOrWhiteSpace(PostCmd))
                PostCmd = "ls -al /tmp";
        }

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
        /// to respond to S3 notifications.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            if (!File.Exists("/tmp/ffmpeg"))
            {
                ExecuteCommand("cp /var/task/ffmpeg /tmp/.; chmod 755 /tmp/ffmpeg;");
            }

            foreach (var record in evnt.Records)
            {
                Console.WriteLine($"New S3 Object {record.S3.Bucket.Name}:{record.S3.Object.Key}");

                if (!File.Exists($"/tmp/{record.S3.Object.Key}"))
                {
                    var request = new GetObjectRequest
                    {
                        BucketName = record.S3.Bucket.Name,
                        Key = record.S3.Object.Key
                    };

                    using (var response = await S3Client.GetObjectAsync(request))
                    {
                        await response.WriteResponseStreamToFileAsync($"/tmp/{record.S3.Object.Key}", false, CancellationToken.None);
                    }
                }

                try
                {
                    ExecuteCommand($"{PreCmd}");

                    Console.WriteLine("Transcoding...");

                    var arg = $"-y -i /tmp/{record.S3.Object.Key} " +
                              $"{FilterPara} " +
                              $"{VideoEncPara} " +
                              $"{AudioEncPara} " +
                              $"{OptsPara} " +
                              $"/tmp/{record.S3.Object.Key}.mp4";
                    Console.WriteLine($"ffmpeg {arg}");
                    var ffmpegProc = new Process
                    {
                        StartInfo = new ProcessStartInfo
                        {
                            FileName = "/tmp/ffmpeg",
                            Arguments = arg,
                            RedirectStandardOutput = true,
                            RedirectStandardError = true,
                            RedirectStandardInput = true,
                            CreateNoWindow = true
                        },
                        EnableRaisingEvents = true
                    };

                    // see below for output handler
                    ffmpegProc.ErrorDataReceived += StandardIOHandler;
                    ffmpegProc.OutputDataReceived += StandardIOHandler;

                    ffmpegProc.Start();
                    ffmpegProc.BeginErrorReadLine();
                    ffmpegProc.BeginOutputReadLine();

                    ffmpegProc.WaitForExit();

                    Console.WriteLine("Transcode Successfully");

                    Console.WriteLine("Upload to S3");
                    await S3Client.PutObjectAsync(new PutObjectRequest
                    {
                        BucketName = "com.moiamond.ffpoc",
                        Key = $"{record.S3.Object.Key}.mp4",
                        FilePath = $"/tmp/{record.S3.Object.Key}.mp4"
                    });

                    File.Delete($"/tmp/{record.S3.Object.Key}");
                    File.Delete($"/tmp/{record.S3.Object.Key}.mp4");

                    ExecuteCommand($"{PostCmd}");
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
                finally
                {
                    Console.WriteLine("final");
                }
            }
        }

        private void StandardIOHandler(object sender, DataReceivedEventArgs e)
        {
            // output will be in string e.Data
            Console.WriteLine(e.Data);
        }

        private void ExecuteCommand(string command)
        {
            var proc = new Process
            {
                StartInfo =
                {
                    FileName = "/bin/bash",
                    Arguments = "-c \" " + command + " \"",
                    UseShellExecute = false,
                    RedirectStandardOutput = true
                }
            };
            proc.Start();

            while (!proc.StandardOutput.EndOfStream)
            {
                Console.WriteLine(proc.StandardOutput.ReadLine());
            }
        }
    }
}
