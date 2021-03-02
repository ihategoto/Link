#include <stdio.h>
#include <fcntl.h>
#include <termios.h> 
#include <unistd.h>  
#include <errno.h>   

void main(void)
    {
    int fd;/*File Descriptor*/

    printf("\n +----------------------------------+");
    printf("\n |        Serial Port Read          |");
    printf("\n +----------------------------------+");

    /*------------------------------- Opening the Serial Port -------------------------------*/

    /* Change /dev/ttyUSB0 to the one corresponding to your system */

    fd = open("/dev/ttyS0",O_RDWR | O_NOCTTY);    /* ttyUSB0 is the FT232 based USB2SERIAL Converter   */
    //  fd = open("/dev/ttyUSB0",O_RDWR | O_NOCTTY | O_NDELAY); /* ttyUSB0 is the FT232 based USB2SERIAL Converter   */
                            /* O_RDWR   - Read/Write access to serial port       */
                            /* O_NOCTTY - No terminal will control the process   */
                            /* Open in blocking mode,read will wait              */

    if(fd == -1)                        /* Error Checking */
            printf("\n  Error! in Opening ttyS0  ");
    else
            printf("\n  ttyS0 Opened Successfully ");


    /*---------- Setting the Attributes of the serial port using termios structure --------- */

    struct termios SerialPortSettings;  /* Create the structure                          */

    tcgetattr(fd, &SerialPortSettings); /* Get the current attributes of the Serial port */

    /* Setting the Baud rate */
    cfsetispeed(&SerialPortSettings,B9600); /* Set Read  Speed as 19200                       */
    cfsetospeed(&SerialPortSettings,B9600); /* Set Write Speed as 19200                       */

    /* 8N1 Mode */
    SerialPortSettings.c_cflag &= ~PARENB;   /* Disables the Parity Enable bit(PARENB),So No Parity   */
    SerialPortSettings.c_cflag &= ~CSTOPB;   /* CSTOPB = 2 Stop bits,here it is cleared so 1 Stop bit */
    SerialPortSettings.c_cflag &= ~CSIZE;    /* Clears the mask for setting the data size             */
    SerialPortSettings.c_cflag |=  CS8;      /* Set the data bits = 8                                 */

    SerialPortSettings.c_cflag &= ~CRTSCTS;       /* No Hardware flow Control                         */
    SerialPortSettings.c_cflag |= CREAD | CLOCAL; /* Enable receiver,Ignore Modem Control lines       */ 


    SerialPortSettings.c_iflag &= ~(IXON | IXOFF | IXANY);          /* Disable XON/XOFF flow control both i/p and o/p */
    SerialPortSettings.c_iflag &= ~(ICANON | ECHO | ECHOE | ISIG);  /* Non Cannonical mode                            */

    SerialPortSettings.c_oflag &= ~OPOST;/*No Output Processing*/

    /* Setting Time outs */
    SerialPortSettings.c_cc[VMIN] = 13; /* Read at least 10 characters */
    SerialPortSettings.c_cc[VTIME] = 0; /* Wait indefinetly   */


    if((tcsetattr(fd,TCSANOW,&SerialPortSettings)) != 0) /* Set the attributes to the termios structure*/
        printf("\n  ERROR ! in Setting attributes");
    else
                printf("\n  BaudRate = 9600 \n  StopBits = 1 \n  Parity   = none");

    char read_buffer[6];   
    int  bytes_read = 0;
    char payload[5];
    payload[0]=0;
    payload[1]=0;
    payload[2]=0;
    payload[3]=1;
    payload[4]=0;
    tcflush(fd, TCIFLUSH);   /* Discards old data in the rx buffer        */
    tcflush(fd, TCOFLUSH);
    int bytes_written = write(fd, payload, 5);
    if(bytes_written == -1){
        perror("Errore");
    }
    sleep(5);
    bytes_read = read(fd, &read_buffer, 6);
    for(int i = 0; i<6; i++)
        printf("%d ", read_buffer[i]);
    printf("\n");
    close(fd); /* Close the serial port */

    }
