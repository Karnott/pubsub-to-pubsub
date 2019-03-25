package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/karnott/pubsub-to-pubsub/util"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"

	"cloud.google.com/go/pubsub"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	// param names
	paramConfig                           = "config"
	paramLogFormat                        = "log-format"
	paramLogLevel                         = "log-level"
	paramFromGoogleCloudProject           = "from-google-cloud-project"
	paramToGoogleCloudProject             = "to-google-cloud-project"
	paramFromGoogleApplicationCredentials = "from-google-application-credentials-json"
	paramToGoogleApplicationCredentials   = "to-google-application-credentials-json"
	paramPubSubSubscription               = "pubsub-subscription"
	paramPubSubDestinationTopic           = "pubsub-destination-topic"

	// default parameters values
	defaultLogLevel  = "debug"
	defaultLogFormat = "json"

	pubSubMaxOutstandingMessages = 10
)

// Config configuration
type Config struct {
	LogFormat                        string
	LogLevel                         string
	FromGoogleCloudProject           string
	ToGoogleCloudProject             string
	FromGoogleApplicationCredentials string
	ToGoogleApplicationCredentials   string
	PubSubSubscription               string
	PubSubDestinationTopic           string
}

var (
	cfgFile string
	cfg     = &Config{}
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "pubsub-to-pubsub",
	Short: "pubsub-to-pubsub",
	Long:  "pubsub-to-pubsub",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()

		util.SetLogger(cfg.LogLevel, cfg.LogFormat)

		logrus.
			WithField(paramConfig, cfgFile).
			WithField(paramLogLevel, cfg.LogLevel).
			WithField(paramLogFormat, cfg.LogFormat).
			WithField(paramFromGoogleCloudProject, cfg.FromGoogleCloudProject).
			WithField(paramToGoogleCloudProject, cfg.ToGoogleCloudProject).
			WithField(paramFromGoogleApplicationCredentials, cfg.FromGoogleApplicationCredentials).
			WithField(paramToGoogleApplicationCredentials, cfg.ToGoogleApplicationCredentials).
			WithField(paramPubSubSubscription, cfg.PubSubSubscription).
			WithField(paramPubSubDestinationTopic, cfg.PubSubDestinationTopic).
			Debug("Configuration")

		if cfg.FromGoogleCloudProject == "" {
			_, _ = fmt.Fprintf(os.Stderr, "FROM_GOOGLE_CLOUD_PROJECT variable must be set.\n")
			os.Exit(1)
		}

		if cfg.ToGoogleCloudProject == "" {
			_, _ = fmt.Fprintf(os.Stderr, "TO_GOOGLE_CLOUD_PROJECT variable must be set.\n")
			os.Exit(1)
		}

		if cfg.PubSubSubscription == "" {
			_, _ = fmt.Fprintf(os.Stderr, "PUBSUB_SUBSCRIPTION variable must be set.\n")
			os.Exit(1)
		}
		if cfg.PubSubDestinationTopic == "" {
			_, _ = fmt.Fprintf(os.Stderr, "PUBSUB_DESTINATION_TOPIC variable must be set.\n")
			os.Exit(1)
		}

		fromCreds, err := google.CredentialsFromJSON(ctx, []byte(cfg.FromGoogleApplicationCredentials), pubsub.ScopePubSub)
		toCreds, err := google.CredentialsFromJSON(ctx, []byte(cfg.ToGoogleApplicationCredentials), pubsub.ScopePubSub)

		if err != nil {
			logrus.Fatalf("Could not find credentials: %v", err)
			os.Exit(1)
		}

		fromClient, err := pubsub.NewClient(ctx, cfg.FromGoogleCloudProject, option.WithCredentials(fromCreds))
		toClient, err := pubsub.NewClient(ctx, cfg.ToGoogleCloudProject, option.WithCredentials(toCreds))

		if err != nil {
			logrus.Fatalf("Could not create pubsub Client: %v", err)
			os.Exit(1)
		}

		sub := fromClient.Subscription(cfg.PubSubSubscription)
		sub.ReceiveSettings.MaxOutstandingMessages = pubSubMaxOutstandingMessages

		topic := toClient.Topic(cfg.PubSubDestinationTopic)

		err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			if _, err = topic.Publish(ctx, msg).Get(ctx); err == nil {
				msg.Ack()
			} else {
				logrus.Errorf("err when inserting data: %v", err)
				msg.Nack()
			}
		})

		if err != nil {
			logrus.Fatal(err)
		}
	},
}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		logrus.Error(err)
		os.Exit(-1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	RootCmd.PersistentFlags().StringVar(&cfgFile, paramConfig, "", "Config file. All flags given in command line will override the values from this file.")
	configureFlag(paramLogFormat, defaultLogFormat, "Log format")
	configureFlag(paramLogLevel, defaultLogLevel, "Log level")
	configureFlag(paramFromGoogleCloudProject, "", "google cloud project where subscription is defined")
	configureFlag(paramToGoogleCloudProject, "", "google cloud project where destination topic is defined")
	configureFlag(paramFromGoogleApplicationCredentials, "", "google cloud credentials to use for subscription access")
	configureFlag(paramToGoogleApplicationCredentials, "", "google cloud credentials to use for publication access")
	configureFlag(paramPubSubSubscription, "", "google cloud subscription")
	configureFlag(paramPubSubDestinationTopic, "", "google cloud destination topic")
}

func configureFlag(flagName, defaultValue, usage string) {
	RootCmd.PersistentFlags().String(flagName, defaultValue, usage)
	_ = viper.BindPFlag(flagName, RootCmd.PersistentFlags().Lookup(flagName))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
	viper.SetConfigFile(viper.GetString(paramConfig))
	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		logrus.Infof("Using config file: %s", viper.ConfigFileUsed())
	}

	cfg.LogFormat = viper.GetString(paramLogFormat)
	cfg.LogLevel = viper.GetString(paramLogLevel)
	cfg.FromGoogleCloudProject = viper.GetString(paramFromGoogleCloudProject)
	cfg.ToGoogleCloudProject = viper.GetString(paramToGoogleCloudProject)
	cfg.FromGoogleApplicationCredentials = viper.GetString(paramFromGoogleApplicationCredentials)
	cfg.ToGoogleApplicationCredentials = viper.GetString(paramToGoogleApplicationCredentials)
	cfg.PubSubSubscription = viper.GetString(paramPubSubSubscription)
	cfg.PubSubDestinationTopic = viper.GetString(paramPubSubDestinationTopic)
}
