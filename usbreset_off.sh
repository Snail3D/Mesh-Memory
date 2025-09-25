#!/bin/bash
PORT=$(lsusb | grep 'WisCore RAK4631' | awk '{print $2"/"$4}' | tr -d ':')
if [ -n "$PORT" ]; then
  sudo usbreset "/dev/bus/usb/${PORT}"
fi
