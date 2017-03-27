package simulations

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/ethereum/go-ethereum/p2p/adapters"

	"golang.org/x/net/context"
)

// ErrExit is the error returned when an exit command is executed
var ErrExit = errors.New("simulator: exit")

// Command is a command that can be executed by the simulator
type Command struct {
	Name string
	Help string
	Args string
	Run  SimulatorFunc
}

// SimulatorFunc is the function which executes a command's actions
type SimulatorFunc func(ctx context.Context, args CommandArgs) error

// CommandArgs are the arguments for a command execution
type CommandArgs map[string]string

// Simulator simulates a devp2p network, providing commands to manipulate nodes
// in the network
type Simulator struct {
	network  *Network
	nodes    map[string]*Node
	commands []*Command
	out      io.Writer
}

func NewSimulator(out io.Writer) *Simulator {
	s := &Simulator{
		network: NewNetwork(&NetworkConfig{}),
		nodes:   make(map[string]*Node),
		out:     out,
	}
	s.network.SetNaf(s.network.NewSimNode)

	// register commands to add, start and stop nodes
	s.Register(&Command{"add", "add new node", "", s.AddNode})
	s.Register(&Command{"start", "start <node>", "<node>", s.StartNode})
	s.Register(&Command{"stop", "stop <node>", "<node>", s.StopNode})

	// register commands to connect and disconnect nodes
	s.Register(&Command{"connect", "connect <nodeA> with <nodeB>", "<nodeA> <nodeB>", s.ConnectNodes})
	s.Register(&Command{"disconnect", "disconnect <nodeA> from <nodeB>", "<nodeA> <nodeB>", s.DisconnectNodes})

	// register commands to inspect the network state
	s.Register(&Command{"list", "list nodes", "", s.ListNodes})
	s.Register(&Command{"info", "display info on <node>", "<node>", s.InspectNode})

	// register helper commands
	s.Register(&Command{"help", "show usage", "", s.Help})
	s.Register(&Command{"exit", "exit the simulator", "", s.Exit})

	return s
}

// Register registers a command which can be executed by the simulator
func (s *Simulator) Register(cmd *Command) {
	s.commands = append(s.commands, cmd)
}

// RunCommand executes the command represented by cmdLine (e.g. "start node1")
func (s *Simulator) RunCommand(ctx context.Context, cmdLine string) error {
	// split cmdLine into the command and args
	parts := strings.SplitN(cmdLine, " ", 2)
	name := parts[0]
	var cmd *Command
	for _, c := range s.commands {
		if c.Name == name {
			cmd = c
			break
		}
	}
	if cmd == nil {
		return fmt.Errorf("command not found: %s", name)
	}
	var args []string
	if len(parts) > 1 {
		args = strings.Split(parts[1], " ")
	}

	// check the args have the correct length
	var argNames []string
	if cmd.Args != "" {
		argNames = strings.Split(cmd.Args, " ")
	}
	if len(args) != len(argNames) {
		return fmt.Errorf("usage: %s %s", cmd.Name, cmd.Args)
	}

	// construct the command arguments
	cmdArgs := make(CommandArgs)
	for i, name := range argNames {
		cmdArgs[name] = args[i]
	}

	// run the command
	return cmd.Run(ctx, cmdArgs)
}

// AddNode adds a new node to the network with a random ID
func (s *Simulator) AddNode(ctx context.Context, args CommandArgs) error {
	id := adapters.RandomNodeId()
	if err := s.network.NewNode(&NodeConfig{Id: id}); err != nil {
		return err
	}
	name := fmt.Sprintf("node%d", len(s.nodes)+1)
	s.nodes[name] = s.network.GetNode(id)
	fmt.Fprintf(s.out, "added %s (%s)\n", name, id)
	return nil
}

// StartNode starts the given node
func (s *Simulator) StartNode(ctx context.Context, args CommandArgs) error {
	name := args["<node>"]
	node, ok := s.nodes[name]
	if !ok {
		return fmt.Errorf("%s does not exist", name)
	}
	if err := s.network.Start(node.Id); err != nil {
		return err
	}
	fmt.Fprintf(s.out, "started %s (%s)\n", name, node.Id)
	return nil
}

// StopNode stops the given node
func (s *Simulator) StopNode(ctx context.Context, args CommandArgs) error {
	name := args["<node>"]
	node, ok := s.nodes[name]
	if !ok {
		return fmt.Errorf("%s does not exist", name)
	}
	if err := s.network.Stop(node.Id); err != nil {
		return err
	}
	fmt.Fprintf(s.out, "stopped %s (%s)\n", name, node.Id)
	return nil
}

// ConnectNodes creates a connection between two nodes in the network
func (s *Simulator) ConnectNodes(ctx context.Context, args CommandArgs) error {
	nameA := args["<nodeA>"]
	nodeA, ok := s.nodes[nameA]
	if !ok {
		return fmt.Errorf("%s does not exist", nameA)
	}
	nameB := args["<nodeB>"]
	nodeB, ok := s.nodes[nameB]
	if !ok {
		return fmt.Errorf("%s does not exist", nameB)
	}
	if err := s.network.Connect(nodeA.Id, nodeB.Id); err != nil {
		return err
	}
	fmt.Fprintf(s.out, "connected %s to %s\n", nameA, nameB)
	return nil
}

// DisconnectNodes severs the connection between two nodes in the network
func (s *Simulator) DisconnectNodes(ctx context.Context, args CommandArgs) error {
	nameA := args["<nodeA>"]
	nodeA, ok := s.nodes[nameA]
	if !ok {
		return fmt.Errorf("%s does not exist", nameA)
	}
	nameB := args["<nodeB>"]
	nodeB, ok := s.nodes[nameB]
	if !ok {
		return fmt.Errorf("%s does not exist", nameB)
	}
	if err := s.network.Disconnect(nodeA.Id, nodeB.Id); err != nil {
		return err
	}
	fmt.Fprintf(s.out, "disconnected %s from %s\n", nameA, nameB)
	return nil
}

// ListNodes list all nodes in the network
func (s *Simulator) ListNodes(ctx context.Context, args CommandArgs) error {
	w := tabwriter.NewWriter(s.out, 0, 8, 0, '\t', 0)
	defer w.Flush()
	fmt.Fprint(w, "NAME\tUP\tID\n")
	for name, node := range s.nodes {
		fmt.Fprintf(w, "%s\t%t\t%s\n", name, node.Up, node.Id)
	}
	return nil
}

// InspectNode displays information on the given node
func (s *Simulator) InspectNode(ctx context.Context, args CommandArgs) error {
	name := args["<node>"]
	node, ok := s.nodes[name]
	if !ok {
		return fmt.Errorf("%s does not exist", name)
	}
	w := tabwriter.NewWriter(s.out, 0, 8, 0, '\t', 0)
	defer w.Flush()
	fmt.Fprintf(w, "ID\t%s\n", node.Id)
	fmt.Fprintf(w, "Up\t%t\n", node.Up)
	return nil
}

// Help displays usage for the simulator
func (s *Simulator) Help(ctx context.Context, args CommandArgs) error {
	w := tabwriter.NewWriter(s.out, 0, 8, 0, '\t', 0)
	defer w.Flush()
	for _, c := range s.commands {
		fmt.Fprintf(w, "%s\t%s\n", c.Name+" "+c.Args, c.Help)
	}
	return nil
}

// Exit returns ErrExit which signals to the caller to stop running commands
// and exit
func (s *Simulator) Exit(ctx context.Context, args CommandArgs) error {
	return ErrExit
}
